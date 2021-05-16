from uuid import uuid4
from threading import RLock, Event
import time
import logging
import threading

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.autoscaler._private.gcp.config import bootstrap_gcp
from ray.autoscaler._private.gcp.config import MAX_POLLS, POLL_INTERVAL, \
    construct_clients_from_provider_config

logger = logging.getLogger(__name__)

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 8


def wait_for_compute_zone_operation(compute, project_name, operation, zone):
    """Poll for compute zone operation until finished."""
    logger.info("wait_for_compute_zone_operation: "
                "Waiting for operation {} to finish...".format(
                    operation["name"]))

    for _ in range(MAX_POLLS):
        result = compute.zoneOperations().wait(
            project=project_name, operation=operation["name"],
            zone=zone).execute()
        if "error" in result:
            raise Exception(result["error"])

        if result["status"] == "DONE":
            logger.info("wait_for_compute_zone_operation: "
                        "Operation {} finished.".format(operation["name"]))
            break

        time.sleep(POLL_INTERVAL)

    return result


class GCPNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        self.lock = RLock()
        _, _, self.compute = construct_clients_from_provider_config(
            provider_config)

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

        # Update labels on a background thread (speedup)
        self.label_lock = RLock()
        self.cur_labels = {}
        self.pending_commit = {}
        threading.Thread(name='_label_updater_thread', target=self._label_committer_thread, args=(provider_config,)).start()

    def _label_committer_thread(self, provider_config):
        #
        # Ray stores the information about node state into GCP instance
        # labels.  Setting these labels takes time (2-3 seconds, each), and
        # if done synchronously this significantly slows down setup of large
        # clusters (~10+ of minutes instead of a ~minute).  We therefore
        # locally accumulate the changes made to labels in set_node_tags(),
        # and asynchronously update the state on GCP in this background
        # thread.
        #

        # construct a new API object for this thread, as these are not thread safe
        _, _, compute = construct_clients_from_provider_config(provider_config)

        project_id = self.provider_config["project_id"]
        availability_zone = self.provider_config["availability_zone"]

        # run until the program exits _and_ we have no more updated labels to commit
        while threading.main_thread().is_alive() or self.cur_labels:
            #
            # initiate commits for locally modified labels
            #
            operations = {}
            with self.label_lock:
                if self.cur_labels:
                    # list the instances with labels to change to get an up-do-date
                    # labelFingerprint
                    eq = [ f'(name = "{node_id}")' for node_id in self.cur_labels.keys() ]
                    filter = " OR ".join(eq)
                    response = compute.instances().list(
                        project=self.provider_config["project_id"],
                        zone=self.provider_config["availability_zone"],
                        filter=filter,
                    ).execute()
                    instances = response.get("items", [])
                    instances = {i["name"]: i for i in instances}

                    # initiate the setLabels operation on all instances
                    for node_id, labels in self.cur_labels.items():
                        operations[node_id] = compute.instances().setLabels(
                            project=project_id,
                            zone=availability_zone,
                            instance=node_id,
                            body={
                                "labels": labels,
                                "labelFingerprint": instances[node_id]["labelFingerprint"]
                            }).execute()

                        # remember what we asked to commit; after the commit is confirmed
                        # we'll use this to verify no addt'l changes were made the meantime.
                        self.pending_commit[node_id] = labels.copy()
            #
            # wait for the pending updates to finish
            #
            for node_id, operation in operations.items():
                result = compute.zoneOperations().wait(
                    project=project_id, operation=operation["name"],
                    zone=availability_zone).execute()

                if "error" in result:
                    logger.error(f"_label_committer_thread: setLabel failed. Info={result}.")
                    continue

                if result["status"] == "DONE":
                    committed_labels = self.pending_commit.pop(node_id)
                    with self.label_lock:
                        # We can delete cur_labels if no new labels have
                        # been added since the commit
                        if self.cur_labels[node_id] == committed_labels:
                            del self.cur_labels[node_id]

            # wait a bit before continuing
            time.sleep(0.1)

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            if tag_filters:
                label_filter_expr = "(" + " AND ".join([
                    "(labels.{key} = {value})".format(key=key, value=value)
                    for key, value in tag_filters.items()
                ]) + ")"
            else:
                label_filter_expr = ""

            instance_state_filter_expr = "(" + " OR ".join([
                "(status = {status})".format(status=status)
                for status in {"PROVISIONING", "STAGING", "RUNNING"}
            ]) + ")"

            cluster_name_filter_expr = ("(labels.{key} = {value})"
                                        "".format(
                                            key=TAG_RAY_CLUSTER_NAME,
                                            value=self.cluster_name))

            not_empty_filters = [
                f for f in [
                    label_filter_expr,
                    instance_state_filter_expr,
                    cluster_name_filter_expr,
                ] if f
            ]

            filter_expr = " AND ".join(not_empty_filters)

            response = self.compute.instances().list(
                project=self.provider_config["project_id"],
                zone=self.provider_config["availability_zone"],
                filter=filter_expr,
            ).execute()

            instances = response.get("items", [])
            # update labels with any uncommitted changes
            with self.label_lock:
                for i in instances:
                    name = i["name"]
                    if name in self.cur_labels:
                        i["labels"]  = self.cur_labels[name]

            # Note: All the operations use "name" as the unique instance id
            self.cached_nodes = {i["name"]: i for i in instances}

            return [i["name"] for i in instances]

    def is_running(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node["status"] == "RUNNING"

    def is_terminated(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node["status"] not in {"PROVISIONING", "STAGING", "RUNNING"}

    def node_tags(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            labels = node.get("labels", {})
            return labels

    def set_node_tags(self, node_id, tags):
        logger.info(f"set_node_tags: node={node_id}, tags={tags}")
        # note: these two locks _must_ be locked in _exactly_ this order,
        # otherwise you'll deadlock with locking in non_terminated_nodes()
        with self.lock:
            with self.label_lock:
                node = self._get_cached_node(node_id)
                newlabels = dict(node["labels"], **tags)

                # update if the labels have changed
                if newlabels != node["labels"]:
                    node["labels"] = self.cur_labels[node_id] = newlabels

    def external_ip(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)

            def get_external_ip(node):
                return node.get("networkInterfaces", [{}])[0].get(
                    "accessConfigs", [{}])[0].get("natIP", None)

            ip = get_external_ip(node)
            if ip is None:
                node = self._get_node(node_id)
                ip = get_external_ip(node)

            return ip

    def internal_ip(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)

            def get_internal_ip(node):
                return node.get("networkInterfaces", [{}])[0].get("networkIP")

            ip = get_internal_ip(node)
            if ip is None:
                node = self._get_node(node_id)
                ip = get_internal_ip(node)

            return ip

    def create_node(self, base_config, tags, count) -> None:
        with self.lock:
            labels = tags  # gcp uses "labels" instead of aws "tags"
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            config = base_config.copy()

            name_label = labels[TAG_RAY_NODE_NAME]
            assert (len(name_label) <=
                    (INSTANCE_NAME_MAX_LEN - INSTANCE_NAME_UUID_LEN - 1)), (
                        name_label, len(name_label))

            machine_type = ("zones/{zone}/machineTypes/{machine_type}"
                            "".format(
                                zone=availability_zone,
                                machine_type=base_config["machineType"]))
            labels = dict(config.get("labels", {}), **labels)

            config.update({
                "machineType": machine_type,
                "labels": dict(labels,
                               **{TAG_RAY_CLUSTER_NAME: self.cluster_name}),
            })

            operations = [
                self.compute.instances().insert(
                    project=project_id,
                    zone=availability_zone,
                    body=dict(
                        config, **{
                            "name": ("{name_label}-{uuid}".format(
                                name_label=name_label,
                                uuid=uuid4().hex[:INSTANCE_NAME_UUID_LEN]))
                        })).execute() for i in range(count)
            ]

            for operation in operations:
                wait_for_compute_zone_operation(self.compute, project_id,
                                                operation, availability_zone)

    def terminate_node(self, node_id):
        with self.lock:
            project_id = self.provider_config["project_id"]
            availability_zone = self.provider_config["availability_zone"]

            operation = self.compute.instances().delete(
                project=project_id,
                zone=availability_zone,
                instance=node_id,
            ).execute()

            result = wait_for_compute_zone_operation(
                self.compute, project_id, operation, availability_zone)

            return result

    def _get_node(self, node_id):
        self.non_terminated_nodes({})  # Side effect: updates cache

        with self.lock:
            if node_id in self.cached_nodes:
                return self.cached_nodes[node_id]

            instance = self.compute.instances().get(
                project=self.provider_config["project_id"],
                zone=self.provider_config["availability_zone"],
                instance=node_id,
            ).execute()

            return instance

    def _get_cached_node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_gcp(cluster_config)
