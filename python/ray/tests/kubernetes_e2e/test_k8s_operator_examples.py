"""Tests launch, teardown, and update of multiple Ray clusters using Kubernetes
operator. Also tests submission of jobs via Ray client."""
import copy
import sys
import os
import subprocess
import tempfile
import time
import unittest

from contextlib import contextmanager

import kubernetes
import pytest
import yaml

import ray

from ray.autoscaler._private._kubernetes.node_provider import\
    KubernetesNodeProvider

IMAGE_ENV = "KUBERNETES_OPERATOR_TEST_IMAGE"
IMAGE = os.getenv(IMAGE_ENV, "rayproject/ray:nightly")

NAMESPACE_ENV = "KUBERNETES_OPERATOR_TEST_NAMESPACE"
NAMESPACE = os.getenv(NAMESPACE_ENV, "test-k8s-operator")

PULL_POLICY_ENV = "KUBERNETES_OPERATOR_TEST_PULL_POLICY"
PULL_POLICY = os.getenv(PULL_POLICY_ENV, "Always")

RAY_PATH = os.path.abspath(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))


@contextmanager
def client_connect_to_k8s(port="10001"):
    command = f"kubectl -n {NAMESPACE}"\
        f" port-forward service/example-cluster-ray-head {port}:{port}"
    command = command.split()
    print(">>>Port-forwarding head service.")
    proc = subprocess.Popen(command)
    # Wait a bit for the port-forwarding connection to be
    # established.
    time.sleep(10)
    ray.util.connect(f"127.0.0.1:{port}")
    try:
        yield proc
    finally:
        proc.kill()


def retry_until_true(f):
    # Retry 60 times with 1 second delay between attempts.
    def f_with_retries(*args, **kwargs):
        for _ in range(240):
            if f(*args, **kwargs):
                return
            else:
                time.sleep(1)
        pytest.fail("The condition wasn't met before the timeout expired.")

    return f_with_retries


@retry_until_true
def wait_for_pods(n, namespace=NAMESPACE):
    client = kubernetes.client.CoreV1Api()
    pods = client.list_namespaced_pod(namespace=namespace).items
    # Double-check that the correct image is use.
    for pod in pods:
        assert pod.spec.containers[0].image == IMAGE
    return len(pods) == n


@retry_until_true
def wait_for_logs(operator_pod):
    """Check if logs indicate presence of nodes of types "head-node" and
    "worker-nodes" in the "example-cluster" cluster."""
    cmd = f"kubectl -n {NAMESPACE} logs {operator_pod}"\
        f"| grep ^example-cluster,{NAMESPACE}: | tail -n 100"
    log_tail = subprocess.check_output(cmd, shell=True).decode()
    return ("head-node" in log_tail) and ("worker-node" in log_tail)


@retry_until_true
def wait_for_job(job_pod):
    print(">>>Checking job logs.")
    cmd = f"kubectl -n {NAMESPACE} logs {job_pod}"
    try:
        out = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
        print(">>>Failed to check job logs.")
        print(e.output.decode())
        return False
    success = "success" in out.lower()
    if success:
        print(">>>Job submission succeeded.")
    else:
        print(">>>Job logs do not indicate job sucess:")
        print(out)
    return success


@retry_until_true
def wait_for_command_to_succeed(cmd):
    try:
        subprocess.check_call(cmd, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


@retry_until_true
def wait_for_pod_status(pod_name, status):
    client = kubernetes.client.CoreV1Api()
    pod = client.read_namespaced_pod(namespace=NAMESPACE, name=pod_name)
    return pod.status.phase == status


@retry_until_true
def wait_for_status(cluster_name, status):
    client = kubernetes.client.CustomObjectsApi()
    cluster_cr = client.get_namespaced_custom_object(
        namespace=NAMESPACE,
        group="cluster.ray.io",
        version="v1",
        plural="rayclusters",
        name=cluster_name)
    return cluster_cr["status"]["phase"] == status


def kubernetes_configs_directory():
    relative_path = "python/ray/autoscaler/kubernetes"
    return os.path.join(RAY_PATH, relative_path)


def get_kubernetes_config_path(name):
    return os.path.join(kubernetes_configs_directory(), name)


def get_operator_config_path(file_name):
    operator_configs = get_kubernetes_config_path("operator_configs")
    return os.path.join(operator_configs, file_name)


def pods():
    client = kubernetes.client.CoreV1Api()
    pod_items = client.list_namespaced_pod(namespace=NAMESPACE).items
    return [
        pod.metadata.name for pod in pod_items
        if pod.status.phase in ["Running", "Pending"]
        and pod.metadata.deletion_timestamp is None
    ]


def num_services():
    cmd = f"kubectl -n {NAMESPACE} get services --no-headers -o"\
        " custom-columns=\":metadata.name\""
    service_list = subprocess.check_output(cmd, shell=True).decode().split()
    return len(service_list)


class KubernetesOperatorTest(unittest.TestCase):
    def test_examples(self):
        # Validate terminate_node error handling
        provider = KubernetesNodeProvider({
            "namespace": NAMESPACE
        }, "default_cluster_name")
        # 404 caught, no error
        provider.terminate_node("no-such-node")

        with tempfile.NamedTemporaryFile("w+") as example_cluster_file, \
                tempfile.NamedTemporaryFile("w+") as example_cluster2_file,\
                tempfile.NamedTemporaryFile("w+") as operator_file,\
                tempfile.NamedTemporaryFile("w+") as job_file:

            # Get paths to operator configs
            example_cluster_config_path = get_operator_config_path(
                "example_cluster.yaml")
            example_cluster2_config_path = get_operator_config_path(
                "example_cluster2.yaml")
            operator_config_path = get_operator_config_path(
                "operator_namespaced.yaml")
            job_path = os.path.join(RAY_PATH,
                                    "doc/kubernetes/job-example.yaml")

            # Load operator configs
            example_cluster_config = yaml.safe_load(
                open(example_cluster_config_path).read())
            example_cluster2_config = yaml.safe_load(
                open(example_cluster2_config_path).read())
            operator_config = list(
                yaml.safe_load_all(open(operator_config_path).read()))
            job_config = yaml.safe_load(open(job_path).read())

            # Fill image fields
            podTypes = example_cluster_config["spec"]["podTypes"]
            podTypes2 = example_cluster2_config["spec"]["podTypes"]
            pod_specs = ([operator_config[-1]["spec"]["template"]["spec"]] + [
                job_config["spec"]["template"]["spec"]
            ] + [podType["podConfig"]["spec"] for podType in podTypes
                 ] + [podType["podConfig"]["spec"] for podType in podTypes2])
            for pod_spec in pod_specs:
                pod_spec["containers"][0]["image"] = IMAGE
                pod_spec["containers"][0]["imagePullPolicy"] = PULL_POLICY

            # Use a custom Redis port for one of the clusters.
            example_cluster_config["spec"]["headStartRayCommands"][1] += \
                " --port 6400"
            example_cluster_config["spec"]["workerStartRayCommands"][1] = \
                " ulimit -n 65536; ray start --address=$RAY_HEAD_IP:6400"

            # Dump to temporary files
            yaml.dump(example_cluster_config, example_cluster_file)
            yaml.dump(example_cluster2_config, example_cluster2_file)
            yaml.dump(job_config, job_file)
            yaml.dump_all(operator_config, operator_file)
            files = [
                example_cluster_file, example_cluster2_file, operator_file
            ]
            for file in files:
                file.flush()

            # Start operator and two clusters
            print("\n>>>Starting operator and two clusters.")
            for file in files:
                cmd = f"kubectl -n {NAMESPACE} apply -f {file.name}"
                subprocess.check_call(cmd, shell=True)

            # Check that autoscaling respects minWorkers by waiting for
            # six pods in the namespace.
            print(">>>Waiting for pods to join clusters.")
            wait_for_pods(6)
            # Check that head services are present.
            print(">>>Checking that head services are present.")
            assert num_services() == 2

            # Check that logging output looks normal (two workers connected to
            # ray cluster example-cluster.)
            operator_pod = [pod for pod in pods() if "operator" in pod].pop()
            wait_for_logs(operator_pod)

            print(">>>Checking that Ray client connection is uninterrupted by"
                  " operator restart.")
            with client_connect_to_k8s():

                @ray.remote
                class Test:
                    @ray.method()
                    def method(self):
                        return "success"

                actor = Test.remote()
                print(">>>Restarting operator pod.")
                cmd = f"kubectl -n {NAMESPACE} delete pod {operator_pod}"
                subprocess.check_call(cmd, shell=True)
                wait_for_pods(6)
                operator_pod = [pod for pod in pods()
                                if "operator" in pod].pop()
                wait_for_pod_status(operator_pod, "Running")
                time.sleep(5)
                print(">>>Confirming Ray is uninterrupted.")
                assert ray.get(actor.method.remote()) == "success"

            # Delete head node of the first cluster. Recovery logic should
            # allow the rest of the test to pass.
            print(">>>Deleting cluster's head to test recovery.")
            head_pod = [pod for pod in pods() if "r-ray-head" in pod].pop()
            cd = f"kubectl -n {NAMESPACE} delete pod {head_pod}"
            subprocess.check_call(cd, shell=True)
            print(">>>Confirming recovery.")
            # Status marked "Running".
            wait_for_status("example-cluster", "Running")
            # Head pod recovered.
            wait_for_pods(6)

            # Get the new head pod
            head_pod = [pod for pod in pods() if "r-ray-head" in pod].pop()
            wait_for_pod_status(head_pod, "Running")
            stat_cmd = f"kubectl -n {NAMESPACE} exec {head_pod} -- ray status"
            print(">>>Waiting for success of `ray status` on recovered head.")
            wait_for_command_to_succeed(stat_cmd)
            print(">>>Stopping ray on the head node to test recovery.")
            stop_cmd = f"kubectl -n {NAMESPACE} exec {head_pod} -- ray stop"
            subprocess.check_call(stop_cmd, shell=True)
            # ray status should fail when called immediately after ray stop
            with pytest.raises(subprocess.CalledProcessError):
                subprocess.check_call(stat_cmd, shell=True)
            print(">>>Waiting for success of `ray status` on recovered head.")
            wait_for_command_to_succeed(stat_cmd)

            # Delete the second cluster
            print(">>>Deleting example-cluster2.")
            cmd = f"kubectl -n {NAMESPACE} delete -f"\
                f"{example_cluster2_file.name}"
            subprocess.check_call(cmd, shell=True)

            # Four pods remain
            print(">>>Checking that example-cluster2 pods are gone.")
            wait_for_pods(4)
            # Cluster 2 service has been garbage-collected.
            print(">>>Checking that deleted cluster's service is gone.")
            assert num_services() == 1

            # Check job submission
            print(">>>Submitting a job to test Ray client connection.")
            cmd = f"kubectl -n {NAMESPACE} create -f {job_file.name}"
            subprocess.check_call(cmd, shell=True)
            job_pod = [pod for pod in pods() if "job" in pod].pop()
            time.sleep(10)
            wait_for_job(job_pod)
            cmd = f"kubectl -n {NAMESPACE} delete jobs --all"
            subprocess.check_call(cmd, shell=True)

            # Check that cluster updates work: increase minWorkers to 3
            # and check that one worker is created.
            print(">>>Updating cluster size.")
            example_cluster_edit = copy.deepcopy(example_cluster_config)
            example_cluster_edit["spec"]["podTypes"][1]["minWorkers"] = 3
            yaml.dump(example_cluster_edit, example_cluster_file)
            example_cluster_file.flush()
            cm = f"kubectl -n {NAMESPACE} apply -f {example_cluster_file.name}"
            subprocess.check_call(cm, shell=True)
            print(">>>Checking that new cluster size is respected.")
            wait_for_pods(5)

            # Delete the first cluster
            print(">>>Deleting second cluster.")
            cmd = f"kubectl -n {NAMESPACE} delete -f"\
                f"{example_cluster_file.name}"
            subprocess.check_call(cmd, shell=True)

            # Only operator pod remains.
            print(">>>Checking that all Ray cluster pods are gone.")
            wait_for_pods(1)

            # Cluster 1 service has been garbage-collected.
            print(">>>Checking that all Ray cluster services are gone.")
            assert num_services() == 0


if __name__ == "__main__":
    kubernetes.config.load_kube_config()
    sys.exit(pytest.main(["-sv", __file__]))
