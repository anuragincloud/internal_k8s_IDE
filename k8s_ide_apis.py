from flask import Blueprint, Flask, jsonify, request
import os
import json
from logging_tools import logging_function
from kubernetes import client, config
import time
import traceback
import mysql.connector
import datetime
import boto3
from botocore.exceptions import ClientError
import datetime as dt
from pb_piescale_emr import get_aws_secret_value
from kubernetes.client.rest import ApiException



pb_piescale_kub = Blueprint('pb_piescale_kub', __name__)


def update_job_history(db_prefix,run_id, job_id=False):
    # db_secret_name
    secret_key_accessor = os.getenv("db_secret_name")
    aws_region=os.getenv('region')
    db_secret_value =  get_aws_secret_value(secret_key_accessor, aws_region)

    db_config = {
        "host": db_secret_value["base-url"].split(":")[-2].lstrip("/").lstrip("/"),
        "user": db_secret_value["username"],
        "password": db_secret_value["password"],
        "database": f"{db_prefix}settings"
    }

    # Establish database connection
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    end_time = dt.datetime.now(dt.timezone.utc).isoformat()
    if job_id:
        update_query = "UPDATE emr_job_runs_status SET status = 'Cancelled', end_time = %s WHERE run_id = %s;"
    else:
        print(run_id)
        update_query = "UPDATE emr_job_runs_status SET status = 'Cancelled', end_time = %s WHERE label = %s;"
    cursor.execute(update_query, (end_time, run_id,))
    connection.commit()
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    except:pass    

def get_child_pod_labels_for_parent(db_prefix, parent_run_label):
    labels = []  # Initialize an empty list to store run_ids
    try:
        # db_secret_name
        secret_key_accessor = os.getenv("db_secret_name")
        aws_region=os.getenv('region')
        db_secret_value =  get_aws_secret_value(secret_key_accessor, aws_region)

        db_config = {
            "host": db_secret_value["base-url"].split(":")[-2].lstrip("/").lstrip("/"),
            "user": db_secret_value["username"],
            "password": db_secret_value["password"],
            "database": f"{db_prefix}settings"
        }

        # Establish database connection
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        select_query = f"""
            SELECT label 
            FROM emr_job_runs_status WHERE parent_run_label = '{parent_run_label}';
        """
        print("select_query : ", select_query)
        cursor.execute(select_query)
        rows = cursor.fetchall()

        for row in rows:  # Loop through each row
            labels.append(row[0])
    except:
        print(traceback.format_exc())
    return labels

    
@pb_piescale_kub.route('/cancel_pod_job', methods=['POST'])
def cancel_pod_job():
    try:
        request_data = json.loads(request.data)
        # Extract parameters from request data
        job_id = request_data.get('job_id', "")
        namespace = request_data['name_space']
        db_prefix = request_data['db_prefix']
        job_label = request_data.get('job_label', "")

        # Initialize Kubernetes client
        try :
            config.load_incluster_config()
        except:
            config.load_kube_config()
        batch_v1 = client.BatchV1Api()

        labels = [job_label]
        child_labels = get_child_pod_labels_for_parent(db_prefix, job_label)

        labels.extend(child_labels)

        print("labels : ", labels)

        if job_id:
            # Create label selector for the job
            label_selector = f"controller-uid={job_id}"
            update_job_history(db_prefix,job_id, job_id=True)
            # List jobs matching the label selector
            jobs = batch_v1.list_namespaced_job(
                namespace=namespace,
                label_selector=label_selector
            )
            if not jobs.items:
                return {"response": "No jobs found"}

            # Delete each matching job
            for job in jobs.items:
                delete_namespace_pod(job.metadata.name, namespace)
        for label in labels:
            print(label)
            if label:
                update_job_history(db_prefix,label)
                delete_namespace_pod(label, namespace)
        return {"response": "success"}

    except Exception as e:
        print(f"Error deleting job: {str(e)}")
        return jsonify({'error': 'An unexpected error occurred'}), 400

def delete_namespace_pod(job_label, namespace):
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    delete_options = client.V1DeleteOptions(propagation_policy='Foreground')
    response = batch_v1.delete_namespaced_job(
        name=job_label,
        namespace=namespace,
        body=delete_options
    )
    return response

def get_autoscaling_client():
    return boto3.client('autoscaling')

def get_eks_client():
    return boto3.client('eks')

def list_nodegroups(cluster_name):
    try:
        eks_client = get_eks_client()
        response = eks_client.list_nodegroups(clusterName=cluster_name)
        nodegroup_names = response['nodegroups']
        nodegroups_info = []
        for ng_name in nodegroup_names:
            ng_details = eks_client.describe_nodegroup(
                clusterName=cluster_name,
                nodegroupName=ng_name
            )['nodegroup']
            if ng_details['status'] == 'ACTIVE':
                nodegroups_info.append({
                    'nodegroup_name': ng_name,
                    'status': ng_details['status'],
                    'instance_types': ng_details['instanceTypes'],
                    'desired_size': ng_details['scalingConfig']['desiredSize'],
                    'min_size': ng_details['scalingConfig']['minSize'],
                    'max_size': ng_details['scalingConfig']['maxSize'],
                    'asg_name': ng_details['resources']['autoScalingGroups'][0]['name']
                })
        return nodegroups_info
    except Exception as e:
        return {'error': str(e)}
    
@pb_piescale_kub.route('/eks_scale', methods=['POST'])
def scale_cluster():
    try:
        data = request.get_json()
        cluster_name = data['cluster_name']
        desired_size = int(data['desired_size'])
        # Get nodegroup info
        eks_client = get_eks_client()
        res = list_nodegroups(cluster_name)
        for info in res:
            if 'error' in info:
                return jsonify(info), 500
            nodegroup_name = info.get("nodegroup_name")
            response = eks_client.describe_nodegroup(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name
            )
            # Get ASG name
            asg_name = response['nodegroup']['resources']['autoScalingGroups'][0]['name']
            # Scale the ASG
            autoscaling_client = get_autoscaling_client()
            autoscaling_client.update_auto_scaling_group(
                AutoScals_statusingGroupName=asg_name,
                DesiredCapacity=desired_size
            )
        return jsonify({
            'message': 'Scaling operation initiated',
            'cluster': cluster_name,
            'nodegroup': nodegroup_name,
            'new_size': desired_size
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@pb_piescale_kub.route('/ek', methods=['POST'])
def get_cluster_status():
    try:
        final_res = []
        data = request.get_json()
        cluster_name = data.get('cluster_name')
        res = list_nodegroups(cluster_name)
        for info in res:
            if 'error' in info:
                return jsonify(info), 500
            nodegroup_name = info.get("nodegroup_name")
            eks_client = get_eks_client()
            response = eks_client.describe_nodegroup(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name
            )
            final_res.append({
                'cluster': cluster_name,
                'nodegroup': nodegroup_name,
                'status': response['nodegroup']['status'],
                'current_size': response['nodegroup']['scalingConfig']['desiredSize']
            })
        return jsonify(final_res)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

#this code block written outside because of latency issues

def get_cluster_arn(cluster_name):
    """ Fetches the ARN of the given EKS cluster from AWS. """
    if cluster_name.startswith("arn:aws:eks"): 
        return cluster_name
    
    try:
        eks_client = boto3.client('eks', region_name='us-east-1')
        response = eks_client.describe_cluster(name=cluster_name)
        return response['cluster']['arn']
    except Exception as e:
        print(f"Error fetching ARN for cluster '{cluster_name}': {e}")
        return f"arn:aws:eks:unknown-cluster"
    
def get_cluster_version(cluster_name):
    try:
        v1 = client.VersionApi()
        version_info = v1.get_code()
        version = f"{version_info.major}.{version_info.minor}"
        return version
    except Exception as e:
        print(f"Error fetching version for {cluster_name}: {e}")
        return f"unknown (error: {str(e)})"

def get_cluster_info():
    in_cluster = os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token')
    
    try:
        if in_cluster:
            config.load_incluster_config()
            api_client = client.VersionApi()
            version_info = api_client.get_code()
            
            cluster_name = os.getenv("EKS_CLUSTER_NAME", "default-cluster")  
            cluster_arn = get_cluster_arn(cluster_name)

            clusters = [{
                # "name": cluster_arn,
                "source": "in-cluster",
                "labels": f"file={config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION}",
                "version": f"{version_info.major}.{version_info.minor}",
                "distro": "eks", 
                "status": "connected"
            }]
        else:
            config.load_kube_config()
            contexts, active_context = config.list_kube_config_contexts()
            clusters = []

            for context in contexts:
                cluster_name = context['context'].get('cluster', 'unknown')
                cluster_arn = get_cluster_arn(cluster_name)

                cluster_info = {
                    # "name": cluster_arn,
                    "source": "local",
                    "labels": f"file={config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION}",
                    "version": get_cluster_version(cluster_name),
                    "distro": "eks",
                    "status": "connected"  
                }
                clusters.append(cluster_info)
    except Exception as e:
        print(f"Failed to load Kubernetes configuration: {e}")
        clusters = []
        
    return clusters
clusters = get_cluster_info()

@pb_piescale_kub.route('/list_cluster_config', methods=['POST'])
def list_cluster_config():
    try:
        return jsonify(clusters), 200
    except Exception as e:
        return jsonify({'error': f"Error loading kubeconfig: {str(e)}"}), 400
    
@pb_piescale_kub.route('/get_namespaces', methods=['POST'])
def get_namespaces():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        v1 = client.CoreV1Api()
        namespaces = v1.list_namespace()
        namespaces_list = []
        for namespace in namespaces.items:
            namespace_info = {
                "name": namespace.metadata.name,
                "labels": namespace.metadata.labels if namespace.metadata.labels else {},
                "age": str((datetime.datetime.now(datetime.timezone.utc) - namespace.metadata.creation_timestamp).days) + " days",
                "status": namespace.status.phase  # 'Active', 'Terminating', etc.
            }
            namespaces_list.append(namespace_info)
        namespaces_list.append({"name": "All namespaces","labels": "","age": "","status": ""})
        return jsonify(namespaces_list), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/nodes', methods=['POST'])
def get_nodes():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()        
        v1 = client.CoreV1Api()
        nodes = v1.list_node()
        nodes_list = [{
            'name': node.metadata.name,
            'status': node.status.conditions[-1].type if node.status.conditions else "Unknown"
        } for node in nodes.items]
        return jsonify(nodes_list), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/list_all_pods', methods=['POST'])
def get_pods():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()
        data = request.get_json() or {}  
        namespace = data.get("namespace", None)

        try:
            limit = int(data.get("page_size", 10))  
        except ValueError:
            limit = 10  
        
        continue_token = data.get("continue_token", None)

        if namespace and namespace != "All namespaces":
            pod_list = v1.list_namespaced_pod(namespace, limit=limit, _continue=continue_token)
        else:
            pod_list = v1.list_pod_for_all_namespaces(limit=limit, _continue=continue_token)

        def format_age(creation_timestamp):
            """ Format timestamp into required age format """
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = int((now - creation_timestamp.replace(tzinfo=datetime.timezone.utc)).total_seconds())

            days = age_seconds // 86400
            hours = (age_seconds % 86400) // 3600
            minutes = (age_seconds % 3600) // 60
            seconds = age_seconds % 60

            if days > 0:
                return f"{days}d"
            elif hours > 0:
                return f"{hours}h"
            elif minutes >= 10:
                return f"{minutes}m"
            elif minutes > 0 or seconds > 0:
                return f"{minutes}m{seconds}s" if minutes > 0 else f"{seconds}s"
            else:
                return "0s"  # Edge case for very recent pods

        pods_list = [
            {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
                "status": pod.status.phase,
                "node": pod.spec.node_name if pod.spec.node_name else "Unknown",
                "age": format_age(pod.metadata.creation_timestamp)
            }
            for pod in pod_list.items
        ]

        next_continue_token = pod_list.metadata._continue if pod_list.metadata._continue else None

        response = {"data": pods_list, "continue_token": next_continue_token}
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/get_node_config', methods=['POST'])
def get_node_config():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()        
        data = request.get_json()
        node_name = data.get("node_name")
        v1 = client.CoreV1Api()
        nodes = v1.list_node()
        for node in nodes.items:
            if node.metadata.name == node_name:
                node_info = {
                    'name': node.metadata.name,
                    'labels': node.metadata.labels,
                    'taints': node.spec.taints,
                    'status': node.status.conditions[-1].type if node.status.conditions else "Unknown",
                    'capacity': node.status.capacity,
                    'allocatable': node.status.allocatable
                }
                return jsonify(node_info), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@pb_piescale_kub.route('/get_pod_logs', methods=['POST'])
def get_pod_logs():
    data = request.get_json()
    namespace = data.get("namespace")
    pod_name = data.get("pod_name")
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1 = client.CoreV1Api()    
    try:
        logs = v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=None,
            pretty=True,
            timestamps=True,
            tail_lines=1000
        )
        return jsonify({
            'status': 'success',
            'pod_name': pod_name,
            'namespace': namespace,
            'logs': logs
        })
    except client.ApiException as e:
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving logs: {str(e)}",
            'code': e.status
        }), e.status

@pb_piescale_kub.route('/get_jobs', methods=['POST'])
def get_jobs():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    v1_batch = client.BatchV1Api()
    data = request.get_json() or {}
    namespace = data.get("namespace", "default")

    try:
        limit = int(data.get("page_size", 10))
    except ValueError:
        limit = 10

    continue_token = data.get("continue_token", None)

    try:
        if namespace == "All namespaces":
            jobs = v1_batch.list_job_for_all_namespaces(limit=limit, _continue=continue_token)
        else:
            jobs = v1_batch.list_namespaced_job(namespace=namespace, limit=limit, _continue=continue_token)

        def format_age(creation_timestamp):
            """ Format timestamp into required age format """
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = int((now - creation_timestamp.replace(tzinfo=datetime.timezone.utc)).total_seconds())

            days = age_seconds // 86400
            hours = (age_seconds % 86400) // 3600
            minutes = (age_seconds % 3600) // 60
            seconds = age_seconds % 60

            if days > 0:
                return f"{days}d"
            elif hours > 0:
                return f"{hours}h"
            elif minutes >= 10:
                return f"{minutes}m"
            elif minutes > 0 or seconds > 0:
                return f"{minutes}m{seconds}s" if minutes > 0 else f"{seconds}s"
            else:
                return "0s"
            
        job_list = [
            {
                "name": job.metadata.name,
                "namespace": job.metadata.namespace,
                "age": format_age(job.metadata.creation_timestamp),
                "status": job.status.conditions[0].type if job.status.conditions else "Running",
                # "active": job.status.active if job.status.active else 0,
                # "succeeded": job.status.succeeded if job.status.succeeded else 0,
                # "failed": job.status.failed if job.status.failed else 0,
                "completions": f"{job.status.succeeded}/{job.spec.completions}" if job.status.succeeded is not None else "0/1"
            }
            for job in jobs.items
        ]

        next_continue_token = jobs.metadata._continue if jobs.metadata._continue else None

        response = {
            "data": job_list,
            "continue_token": next_continue_token
        }

        return jsonify(response), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving jobs: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/get_cron_jobs', methods=['POST'])
def get_cron_jobs():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")  # Default to 'default' namespace if not provided
    try:
        if namespace == "All namespaces":
            cron_jobs = v1_batch.list_cron_job_for_all_namespaces()
        else:
            cron_jobs = v1_batch.list_namespaced_cron_job(namespace=namespace)
        cron_job_list = [
            {
                "name": cron_job.metadata.name,
                "namespace": cron_job.metadata.namespace,
                "creation_timestamp": cron_job.metadata.creation_timestamp,
                "schedule": cron_job.spec.schedule,
                "active_jobs": len(cron_job.status.active) if cron_job.status.active else 0,
                "last_schedule_time": cron_job.status.last_schedule_time
                if cron_job.status.last_schedule_time
                else "Never"
            }
            for cron_job in cron_jobs.items
        ]
        if not cron_job_list:
            cron_job_list.append({
                "name": "",
                "namespace": "",
                "creation_timestamp": "",
                "schedule": "",
                "active_jobs": 0,
                "last_schedule_time": ""
            })
        return jsonify(cron_job_list), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving cron jobs: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/trigger_cron_job', methods=['POST'])
def trigger_cron_job():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")  # Default to 'default' namespace if not provided
    cron_job_name = data.get("cron_job_name")
    if not cron_job_name:
        return jsonify({
            "status": "error",
            "message": "cron_job_name is required."
        }), 400
    try:
        # Get the CronJob details
        cron_job = v1_batch.read_namespaced_cron_job(name=cron_job_name, namespace=namespace)
        # Generate a unique name for the triggered Job
        job_name = f"{cron_job_name}-manual-{int(time.time())}"
        # Create a Job spec from the CronJob's spec
        job_body = client.V1Job(
            metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
            spec=cron_job.spec.job_template.spec
        )
        # Trigger the Job
        v1_batch.create_namespaced_job(namespace=namespace, body=job_body)
        return jsonify({
            "status": "success",
            "message": f"CronJob '{cron_job_name}' triggered successfully.",
            "job_name": job_name,
            "namespace": namespace
        }), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error triggering CronJob: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/resume_cron_job', methods=['POST'])
def resume_cron_job():
    data = request.get_json()
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    namespace = data.get("namespace", "default")
    cron_job_name = data.get("cron_job_name")
    if not cron_job_name:
        return jsonify({
            "status": "error",
            "message": "cron_job_name is required."
        }), 400
    try:
        # Fetch the CronJob details
        cron_job = v1_batch.read_namespaced_cron_job(name=cron_job_name, namespace=namespace)
        # Check if the CronJob is already active
        if not cron_job.spec.suspend:
            return jsonify({
                "status": "success",
                "message": f"CronJob '{cron_job_name}' is already active."
            }), 200
        # Update the CronJob to set `spec.suspend` to False
        cron_job.spec.suspend = False
        v1_batch.patch_namespaced_cron_job(name=cron_job_name, namespace=namespace, body=cron_job)
        return jsonify({
            "status": "success",
            "message": f"CronJob '{cron_job_name}' resumed successfully.",
            "namespace": namespace
        }), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error resuming CronJob: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/suspend_cron_job', methods=['POST'])
def suspend_cron_job():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()    
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")
    cron_job_name = data.get("cron_job_name")
    if not cron_job_name:
        return jsonify({
            "status": "error",
            "message": "cron_job_name is required."
        }), 400
    try:
        # Fetch the CronJob details
        cron_job = v1_batch.read_namespaced_cron_job(name=cron_job_name, namespace=namespace)
        # Check if the CronJob is already suspended
        if cron_job.spec.suspend:
            return jsonify({
                "status": "success",
                "message": f"CronJob '{cron_job_name}' is already suspended."
            }), 200
        # Update the CronJob to set `spec.suspend` to True
        cron_job.spec.suspend = True
        v1_batch.patch_namespaced_cron_job(name=cron_job_name, namespace=namespace, body=cron_job)
        return jsonify({
            "status": "success",
            "message": f"CronJob '{cron_job_name}' suspended successfully.",
            "namespace": namespace
        }), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error suspending CronJob: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/get_deployments', methods=['POST'])
def get_deployments():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    v1_apps = client.AppsV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")
    try:
        if namespace == "All namespaces":
            deployments = v1_apps.list_deployment_for_all_namespaces()
        else:
            deployments = v1_apps.list_namespaced_deployment(namespace=namespace)
        deployment_list = []
        for deployment in deployments.items:
            # Retrieve basic info for the deployment
            deployment_info = {
                "name": deployment.metadata.name,
                "namespace": deployment.metadata.namespace,
                "pods": deployment.status.available_replicas if deployment.status.available_replicas else 0,
                "replicas": deployment.spec.replicas,
                "age": str((datetime.datetime.now(datetime.timezone.utc) - deployment.metadata.creation_timestamp).days) + " days",
                "conditions": {
                    "Available": next((cond.status for cond in deployment.status.conditions if cond.type == "Available"), "Unknown"),
                    "Progressing": next((cond.status for cond in deployment.status.conditions if cond.type == "Progressing"), "Unknown")
                }
            }
            deployment_list.append(deployment_info)
        return jsonify(deployment_list), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving deployments: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/get_statefulsets', methods=['POST'])
def get_statefulsets():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_apps = client.AppsV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")
    try:
        if namespace == "All namespaces":
            statefulsets = v1_apps.list_stateful_set_for_all_namespaces()
        else:
            statefulsets = v1_apps.list_namespaced_stateful_set(namespace=namespace)
        statefulset_list = []
        for statefulset in statefulsets.items:
            # Retrieve basic info for the statefulset
            statefulset_info = {
                "name": statefulset.metadata.name,
                "namespace": statefulset.metadata.namespace,
                "replicas": statefulset.spec.replicas,
                "pods": statefulset.status.replicas if statefulset.status.replicas else 0,
                "age": str((datetime.datetime.now(datetime.timezone.utc) - statefulset.metadata.creation_timestamp).days) + " days",
            }
            statefulset_list.append(statefulset_info)

        if not statefulset_list:
            statefulset_list.append({
                "name":"",
                "namespace":"",
                "replicas":"",
                "pods":"",
                "age":""
            })

        return jsonify(statefulset_list), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving statefulsets: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/get_daemonsets', methods=['POST'])
def get_daemonsets():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_apps = client.AppsV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")
    try:
        # Fetch the list of DaemonSets in the specified namespace
        daemonsets = v1_apps.list_namespaced_daemon_set(namespace=namespace)
        daemonset_list = []
        for daemonset in daemonsets.items:
            # Retrieve basic info for the DaemonSet
            daemonset_info = {
                "name": daemonset.metadata.name,
                "namespace": daemonset.metadata.namespace,
                "pods": daemonset.status.desired_number_scheduled if daemonset.status.desired_number_scheduled else 0,
                "age": str((datetime.datetime.now(datetime.timezone.utc) - daemonset.metadata.creation_timestamp).days) + " days",
                "node_selector": daemonset.spec.template.spec.node_selector if daemonset.spec.template.spec.node_selector else {}
            }
            daemonset_list.append(daemonset_info)
        if not daemonset_list:
            daemonset_info = {
                "name": "",
                "namespace": "",
                "pods": "",
                "age": "",
                "node_selector": ""
            }
            daemonset_list.append(daemonset_info)
        return jsonify(daemonset_list), 200
    except client.ApiException as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving daemonsets: {str(e)}",
            "code": e.status
        }), e.status
    
@pb_piescale_kub.route('/create_cron_job', methods=['POST'])
def create_cron_job():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    job_name = data.get("job_name")
    namespace = data.get("namespace")
    image = data.get("image", "")
    command = data.get("command")
    schedule = data.get("cron_syntax")
    try:
        # Define the labels
        labels = {"custom-cron-job": "true"}
        cron_job = client.V1CronJob(
            metadata=client.V1ObjectMeta(
                name=job_name,
                labels=labels  # Add labels here
            ),
            spec=client.V1CronJobSpec(
                schedule=schedule,
                job_template=client.V1JobTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels=labels  # Add labels to the job template metadata
                    ),
                    spec=client.V1JobSpec(
                        template=client.V1PodTemplateSpec(
                            metadata=client.V1ObjectMeta(
                                labels=labels  # Add labels to the pod template metadata
                            ),
                            spec=client.V1PodSpec(
                                containers=[
                                    client.V1Container(
                                        name="cron-job-container",
                                        image=image,
                                        command=command,
                                    )
                                ],
                                restart_policy="OnFailure",
                            )
                        )
                    )
                )
            )
        )
        # Create the CronJob
        api_response = v1_batch.create_namespaced_cron_job(
            namespace=namespace,
            body=cron_job
        )
        return jsonify({"response": "success"}), 200
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred - {str(e)}'}), 400
    
@pb_piescale_kub.route('/delete_cron_job', methods=['POST'])
def delete_cron_job():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    job_name = data.get("cron_job_name")
    namespace = data.get("namespace")
    try:
        api_response = v1_batch.delete_namespaced_cron_job(
            name=job_name,
            namespace=namespace,
            body=client.V1DeleteOptions()
        )
        return jsonify({'response': f'CronJob {job_name} deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred - {str(e)}'}), 400
    
@pb_piescale_kub.route('/list_cron_jobs', methods=['POST'])
def list_cron_jobs():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    namespace = data.get("namespace")
    label_selector = data.get("label_selector")
    try:
        # List CronJobs with the specified label
        cron_jobs = v1_batch.list_namespaced_cron_job(
            namespace=namespace,
            label_selector=label_selector
        )
        # Parse and return the CronJobs
        cron_job_list = []
        for cron_job in cron_jobs.items:
            cron_job_list.append({
                "name": cron_job.metadata.name,
                "namespace": cron_job.metadata.namespace,
                "schedule": cron_job.spec.schedule,
                "labels": cron_job.metadata.labels
            })
        return jsonify({"cron_jobs": cron_job_list}), 200
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred - {str(e)}'}), 400
    
@pb_piescale_kub.route('/update_cron_job', methods=['POST'])
def update_cron_job():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_batch = client.BatchV1Api()
    data = request.get_json()
    job_name = data.get("job_name")
    namespace = data.get("namespace")
    new_schedule = data.get("new_cron_syntax")
    try:
        existing_cron_job = v1_batch.read_namespaced_cron_job(
            name=job_name,
            namespace=namespace
        )
        existing_cron_job.spec.schedule = new_schedule
        updated_cron_job = v1_batch.patch_namespaced_cron_job(
            name=job_name,
            namespace=namespace,
            body=existing_cron_job
        )
        return jsonify({
            'response': f'CronJob {job_name} updated successfully',
            'new_schedule': updated_cron_job.spec.schedule
        }), 200
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred - {str(e)}'}), 400
    
@pb_piescale_kub.route('/get_pod_info', methods=['POST'])
def get_pod_info():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")  # Default to 'default' namespace
    pod_name = data.get("pod_name")  # Required pod name
    if not pod_name:
        return jsonify({"status": "error", "message": "Pod name is required"}), 400
    try:
        # Fetch pod details
        pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
        # Fetch events related to the pod
        events = v1.list_namespaced_event(namespace=namespace, field_selector=f"involvedObject.name={pod_name}")
        # Fetch logs (Last 100 lines for brevity)
        try:
            logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace, tail_lines=100)
        except client.ApiException:
            logs = "No logs available"
        # Extract relevant pod information
        pod_info = {
            "name": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "labels": pod.metadata.labels,
            "annotations": pod.metadata.annotations,
            "node": pod.spec.node_name,
            "status": pod.status.phase,
            "pod_ip": pod.status.pod_ip,
            "host_ip": pod.status.host_ip,
            "qos_class": pod.status.qos_class,  # QoS class (BestEffort, Burstable, Guaranteed)
            "service_account": pod.spec.service_account_name,  # Service account name
            "created_at": pod.metadata.creation_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "restart_policy": pod.spec.restart_policy,  # Restart policy (Always, Never, OnFailure)
            "owner_references": [
                {"name": owner.name, "kind": owner.kind, "uid": owner.uid}
                for owner in pod.metadata.owner_references
            ] if pod.metadata.owner_references else "No owner references",
            "containers": [
                {
                    "name": container.name,
                    "image": container.image,
                    "ports": [port.container_port for port in container.ports] if container.ports else [],
                    "resources": {
                        "requests": container.resources.requests if container.resources and container.resources.requests else "Not specified",
                        "limits": container.resources.limits if container.resources and container.resources.limits else "Not specified"
                    },
                    "env_variables": [
                        {"name": env.name, "value": env.value} for env in container.env
                    ] if container.env else [],
                    "volume_mounts": [
                        {"mount_path": vm.mount_path, "name": vm.name}
                        for vm in container.volume_mounts
                    ] if container.volume_mounts else "No volume mounts"
                }
                for container in pod.spec.containers
            ],
            "init_containers": [
                {
                    "name": init_container.name,
                    "image": init_container.image,
                    "resources": {
                        "requests": init_container.resources.requests if init_container.resources and init_container.resources.requests else "Not specified",
                        "limits": init_container.resources.limits if init_container.resources and init_container.resources.limits else "Not specified"
                    }
                }
                for init_container in pod.spec.init_containers
            ] if pod.spec.init_containers else "No init containers",
            "conditions": [
                {
                    "type": cond.type,
                    "status": cond.status,
                    "last_transition_time": cond.last_transition_time.strftime("%Y-%m-%d %H:%M:%S")
                    if cond.last_transition_time else "N/A"
                }
                for cond in pod.status.conditions
            ],
            "restart_count": sum(container.restart_count for container in pod.status.container_statuses)
            if pod.status.container_statuses else 0,
            "tolerations": [
                {
                    "key": tol.key,
                    "operator": tol.operator,
                    "value": tol.value,
                    "effect": tol.effect
                }
                for tol in pod.spec.tolerations
            ] if pod.spec.tolerations else "No tolerations",
            "affinity": {
                "node_affinity": pod.spec.affinity.node_affinity if pod.spec.affinity and pod.spec.affinity.node_affinity else "No node affinity",
                "pod_affinity": pod.spec.affinity.pod_affinity if pod.spec.affinity and pod.spec.affinity.pod_affinity else "No pod affinity",
                "pod_anti_affinity": pod.spec.affinity.pod_anti_affinity if pod.spec.affinity and pod.spec.affinity.pod_anti_affinity else "No pod anti-affinity"
            },
            "volumes": [
                {
                    "name": volume.name,
                    "persistentVolumeClaim": volume.persistent_volume_claim.claim_name if volume.persistent_volume_claim else "Not a PVC"
                }
                for volume in pod.spec.volumes
            ] if pod.spec.volumes else "No volumes",
            "events": [
                {
                    "reason": event.reason,
                    "message": event.message,
                    "count": event.count,
                    "timestamp": event.last_timestamp.strftime("%Y-%m-%d %H:%M:%S") if event.last_timestamp else "N/A"
                }
                for event in events.items
            ] if events.items else "No events",
            "logs": logs  # Last 100 lines of logs
        }
        return jsonify({"status": "success", "pod_info": pod_info}), 200
    except client.ApiException as e:
        return jsonify({"status": "error", "message": f"Error retrieving pod info: {str(e)}", "code": e.status}), e.status
    
@pb_piescale_kub.route('/get_deployment_details', methods=['POST'])
def get_deployment_details():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    v1_apps = client.AppsV1Api()
    data = request.get_json()
    namespace = data.get("namespace", "default")  # Default namespace
    deployment_name = data.get("deployment_name")  # Required deployment name
    if not deployment_name:
        return jsonify({"status": "error", "message": "Deployment name is required"}), 400
    try:
        # Fetch the specific deployment
        deployment = v1_apps.read_namespaced_deployment(name=deployment_name, namespace=namespace)
        # Extract deployment details
        deployment_info = {
            "name": deployment.metadata.name,
            "namespace": deployment.metadata.namespace,
            "labels": deployment.metadata.labels,
            "annotations": deployment.metadata.annotations,
            "replicas": {
                "desired": deployment.spec.replicas,
                "available": deployment.status.available_replicas if deployment.status.available_replicas else 0,
                "updated": deployment.status.updated_replicas if deployment.status.updated_replicas else 0,
                "ready": deployment.status.ready_replicas if deployment.status.ready_replicas else 0,
                "unavailable": deployment.status.unavailable_replicas if deployment.status.unavailable_replicas else 0
            },
            "age": str((datetime.datetime.now(datetime.timezone.utc) - deployment.metadata.creation_timestamp).days) + " days",
            "strategy_type": deployment.spec.strategy.type,
            "conditions": {
                condition.type: condition.status for condition in deployment.status.conditions
            } if deployment.status.conditions else "No conditions available",
            "deployment_revision": deployment.metadata.annotations.get("deployment.kubernetes.io/revision", "Unknown"),
            "selector": deployment.spec.selector.match_labels if deployment.spec.selector else {},
            "containers": [
                {
                    "name": container.name,
                    "image": container.image,
                    "ports": [port.container_port for port in container.ports] if container.ports else [],
                    "resources": {
                        "requests": container.resources.requests if container.resources and container.resources.requests else "Not specified",
                        "limits": container.resources.limits if container.resources and container.resources.limits else "Not specified"
                    },
                    "env_variables": [
                        {"name": env.name, "value": env.value} for env in container.env
                    ] if container.env else []
                }
                for container in deployment.spec.template.spec.containers
            ],
            "node_affinity": deployment.spec.template.spec.affinity.node_affinity if deployment.spec.template.spec.affinity else "Not specified",
            "tolerations": [
                {
                    "key": tol.key,
                    "operator": tol.operator,
                    "value": tol.value,
                    "effect": tol.effect
                }
                for tol in deployment.spec.template.spec.tolerations
            ] if deployment.spec.template.spec.tolerations else "No tolerations",
            "volumes": [
                {
                    "name": volume.name,
                    "persistentVolumeClaim": volume.persistent_volume_claim.claim_name if volume.persistent_volume_claim else "Not a PVC"
                }
                for volume in deployment.spec.template.spec.volumes
            ] if deployment.spec.template.spec.volumes else "No volumes"
        }
        return jsonify({
            "status": "success",
            "namespace": namespace,
            "deployment": deployment_info
        })
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred - {str(e)}'}), 400

@pb_piescale_kub.route('/get_pod_details', methods=['POST']) 
def get_pod_details():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("name_space")
        pod_info = data.get("pod_info")

        if not namespace:
            return jsonify({"error": "Namespace is required"}), 400
        if not pod_info:
            return jsonify({"error": "Pod info is required"}), 400

        pod_details = []

        if namespace == "All namespaces":
            # Search across all namespaces
            all_pods = v1.list_pod_for_all_namespaces()
        else:
            # Search within a specific namespace
            all_pods = v1.list_namespaced_pod(namespace=namespace)
        
        def format_age(creation_timestamp):
            """ Format timestamp into required age format """
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = int((now - creation_timestamp.replace(tzinfo=datetime.timezone.utc)).total_seconds())

            days = age_seconds // 86400
            hours = (age_seconds % 86400) // 3600
            minutes = (age_seconds % 3600) // 60
            seconds = age_seconds % 60

            if days > 0:
                return f"{days}d"
            elif hours > 0:
                return f"{hours}h"
            elif minutes >= 10:
                return f"{minutes}m"
            elif minutes > 0 or seconds > 0:
                return f"{minutes}m{seconds}s" if minutes > 0 else f"{seconds}s"
            else:
                return "0s"  # Edge case for very recent pods

        # Exact name match
        for pod in all_pods.items:
            if pod.metadata.name == pod_info:
                pod_details.append({
                    'name': pod.metadata.name,
                    'namespace': pod.metadata.namespace,
                    'status': pod.status.phase,
                    "node": pod.spec.node_name if pod.spec.node_name else "Unknown",
                    "age": format_age(pod.metadata.creation_timestamp)
                })
                return jsonify({"data": pod_details}), 200

        # Search by label selector
        label_selector = f"controller-uid={pod_info}"
        if namespace == "All namespaces":
            pods = v1.list_pod_for_all_namespaces(label_selector=label_selector)
        else:
            pods = v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)

        for pod in pods.items:
            pod_details.append({
                'name': pod.metadata.name,
                'namespace': pod.metadata.namespace,
                'status': pod.status.phase,
                "node": pod.spec.node_name if pod.spec.node_name else "Unknown",
                "age": format_age(pod.metadata.creation_timestamp)
            })

        # Partial name match (if no exact or label match found)
        if not pod_details:
            for pod in all_pods.items:
                if pod_info in pod.metadata.name:
                    pod_details.append({
                        'name': pod.metadata.name,
                        'namespace': pod.metadata.namespace,
                        'status': pod.status.phase,
                        "node": pod.spec.node_name if pod.spec.node_name else "Unknown",
                        "age": format_age(pod.metadata.creation_timestamp)
                    })

        if pod_details:
            return jsonify({"data": pod_details}), 200
        else:
            return jsonify({"error": "No matching pods found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 400


def calculate_age(creation_timestamp):
    """Calculate the age of the pod from the creation timestamp."""
    if not creation_timestamp:
        return "Unknown"

    created_time = creation_timestamp.replace(tzinfo=datetime.timezone.utc)
    age = (datetime.datetime.now(datetime.timezone.utc)) - created_time

    # Format as days, hours, minutes
    days = age.days
    hours, remainder = divmod(age.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"
    
@pb_piescale_kub.route('/get_config_maps', methods=['POST'])
def get_config_maps():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config() 
        
        # Initialize the CoreV1 API client
        v1 = client.CoreV1Api() 
        data = request.get_json()
        namespace = data.get("namespace")
        limit = int(data.get("page_size", 10))
        continue_token = data.get("continue_token", None)

        if namespace:
            pods = v1.list_namespaced_pod(namespace, limit=limit, _continue=continue_token)
        else:
            pods = v1.list_pod_for_all_namespaces(limit=limit, _continue=continue_token)

        pod_list = [
            {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
                "status": pod.status.phase,  # Running, Pending, Failed, etc.
                "node": pod.spec.node_name,  # Node where the pod is running
                "age": calculate_age(pod.metadata.creation_timestamp),
                "ip": pod.status.pod_ip if pod.status.pod_ip else "N/A",
                "containers": ", ".join([container.name for container in pod.spec.containers]),  # List of container names
                "creation_timestamp": pod.metadata.creation_timestamp
            }
            for pod in pods.items
        ]

        next_continue_token = pods.metadata._continue if pods.metadata._continue else None
        response = {"data": pod_list, "continue_token": next_continue_token}
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400

@pb_piescale_kub.route('/get_secrets', methods=['POST'])
def get_secrets():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config() 
        # Initialize the CoreV1 API client
        v1 = client.CoreV1Api() 
        data = request.get_json()
        namespace = data.get("namespace")
        limit = int(data.get("page_size", 10))
        continue_token = data.get("continue_token", None)

        if namespace:
            secrets = v1.list_namespaced_secret(namespace, limit=limit, _continue=continue_token)
        else:
            secrets = v1.list_secret_for_all_namespaces(limit=limit, _continue=continue_token)

        secret_list = [
            {
                "name": secret.metadata.name,
                "namespace": secret.metadata.namespace,
                "labels": secret.metadata.labels if secret.metadata.labels else {},  # Extract labels
                "keys": ", ".join(secret.data.keys()) if secret.data else "",  # Extract keys as CSV
                "type": secret.type,
                "age": calculate_age(secret.metadata.creation_timestamp),  # Compute age
                "creation_timestamp": secret.metadata.creation_timestamp
            }
            for secret in secrets.items
        ]

        next_continue_token = secrets.metadata._continue if secrets.metadata._continue else None
        response = {"data": secret_list, "continue_token": next_continue_token}
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_hpas', methods=['POST'])
def get_hpas():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config() 
        
        v1 = client.AutoscalingV2Api()  # Use AutoscalingV2 for full metrics support
        data = request.get_json()
        namespace = data.get("namespace", None)

        if namespace == "All namespaces":
            hpas = v1.list_horizontal_pod_autoscaler_for_all_namespaces()
        else:
            hpas = v1.list_namespaced_horizontal_pod_autoscaler(namespace)

        hpa_list = []

        def format_age(creation_timestamp):
            """ Format timestamp into required age format """
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = int((now - creation_timestamp.replace(tzinfo=datetime.timezone.utc)).total_seconds())

            days = age_seconds // 86400
            hours = (age_seconds % 86400) // 3600
            minutes = (age_seconds % 3600) // 60
            seconds = age_seconds % 60

            if days > 0:
                return f"{days}d"
            elif hours > 0:
                return f"{hours}h"
            elif minutes >= 10:
                return f"{minutes}m"
            elif minutes > 0 or seconds > 0:
                return f"{minutes}m{seconds}s" if minutes > 0 else f"{seconds}s"
            else:
                return "0s"  # Edge case for very recent pods
        
        for hpa in hpas.items:
            metrics = []
            if hpa.spec.metrics:
                for metric in hpa.spec.metrics:
                    metric_type = metric.type
                    if metric_type == "Resource":
                        name = metric.resource.name
                        target = metric.resource.target.average_utilization
                        metrics.append(f"{name}: {target}%")
                    elif metric_type == "Pods":
                        name = metric.pods.metric.name
                        target = metric.pods.target.average_value
                        metrics.append(f"{name}: {target}")
                    elif metric_type == "Object":
                        name = metric.object.metric.name
                        target = metric.object.target.average_value
                        metrics.append(f"{name}: {target}")

            hpa_details = {
                "name": hpa.metadata.name,
                "namespace": hpa.metadata.namespace,
                "metrics": ", ".join(metrics) if metrics else "No metrics",
                "min_pods": hpa.spec.min_replicas,
                "max_pods": hpa.spec.max_replicas,
                "replicas": hpa.status.current_replicas,
                "age": format_age(hpa.metadata.creation_timestamp),
                "status": "AbleToScale"
            }
            hpa_list.append(hpa_details)

        return jsonify({"data": hpa_list}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_pdbs', methods=['POST'])
def get_pdbs():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.PolicyV1Api()  # Kubernetes Policy API
        data = request.get_json()
        namespace = data.get("namespace", None)

        if namespace:
            pdbs = v1.list_namespaced_pod_disruption_budget(namespace)
        else:
            pdbs = v1.list_pod_disruption_budget_for_all_namespaces()

        pdb_list = []

        for pdb in pdbs.items:
            min_available = pdb.spec.min_available
            max_unavailable = pdb.spec.max_unavailable

            pdb_details = {
                "name": pdb.metadata.name,
                "namespace": pdb.metadata.namespace,
                "min_available": min_available if min_available else "N/A",
                "max_unavailable": max_unavailable if max_unavailable else "N/A",
                "current_healthy": pdb.status.current_healthy,
                "desired_healthy": pdb.status.desired_healthy,
                "age": calculate_age(pdb.metadata.creation_timestamp)
            }
            pdb_list.append(pdb_details)

        if not pdb_list:
            pdb_list.append({
                "name":"",
                "namespace":"",
                "min_available":"",
                "max_unavailable":"",
                "current_healthy":"",
                "desired_healthy":"",
                "age":""
            })

        return jsonify({"data": pdb_list}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_priority_classes', methods=['POST'])
def get_priority_classes():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.SchedulingV1Api()  # Kubernetes Scheduling API
        priority_classes = v1.list_priority_class()

        priority_list = [
            {
                "name": pc.metadata.name,
                "value": pc.value,
                "global_default": pc.global_default,
                "age": calculate_age(pc.metadata.creation_timestamp)
            }
            for pc in priority_classes.items
        ]

        return jsonify({"data": priority_list}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_leases', methods=['POST'])
def get_leases():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoordinationV1Api()  # Kubernetes Coordination API
        data = request.get_json()
        namespace = data.get("namespace", None)

        if namespace:
            leases = v1.list_namespaced_lease(namespace)
        else:
            leases = v1.list_lease_for_all_namespaces()

        lease_list = [
            {
                "name": lease.metadata.name,
                "namespace": lease.metadata.namespace,
                "holder": lease.spec.holder_identity if lease.spec and lease.spec.holder_identity else "N/A",
                "age": calculate_age(lease.metadata.creation_timestamp)
            }
            for lease in leases.items
        ]
        if not lease_list:
            lease_list.append({
                "name":"",
                "namespace":"",
                "holder":"",
                "age":""
            })
        return jsonify({"data": lease_list}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/get_mutating_webhooks', methods=['POST'])
def get_mutating():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.AdmissionregistrationV1Api()
        webhook_configs = v1.list_mutating_webhook_configuration()
        
        webhook_list = [
            {
                "name": wh.metadata.name,
                "webhooks": ", ".join([whc.name for whc in wh.webhooks]),
                "age": calculate_age(wh.metadata.creation_timestamp)
            }
            for wh in webhook_configs.items
        ]
        
        return jsonify({"data": webhook_list}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@pb_piescale_kub.route('/validate_webhook', methods=['POST'])
def validate_webhook():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.AdmissionregistrationV1Api()
        webhook_configs = v1.list_validating_webhook_configuration()
        
        webhook_list = [
            {
                "name": wh.metadata.name,
                "webhooks": ", ".join([whc.name for whc in wh.webhooks]),
                "age": calculate_age(wh.metadata.creation_timestamp)
            }
            for wh in webhook_configs.items
        ]
        
        return jsonify({"data": webhook_list}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
def get_job_status(status):
    if status.succeeded:
        return "Completed"
    elif status.failed:
        return "Failed"
    elif status.active:
        return "Running"
    return "Unknown"


@pb_piescale_kub.route('/get_job_details', methods=['POST'])
def get_job_details():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1_batch = client.BatchV1Api()
        data = request.get_json()
        namespace = data.get("namespace")
        job_info = data.get("job_info")

        if not namespace:
            return jsonify({"error": "Namespace is required"}), 400
        if not job_info:
            return jsonify({"error": "Job info is required"}), 400

        job_details = []

        if namespace == "All namespaces":
            # Fetch all jobs across all namespaces
            all_jobs = v1_batch.list_job_for_all_namespaces()
        else:
            # Fetch jobs in the specified namespace
            all_jobs = v1_batch.list_namespaced_job(namespace=namespace)

        def format_age(creation_timestamp):
            """ Format timestamp into required age format """
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = int((now - creation_timestamp.replace(tzinfo=datetime.timezone.utc)).total_seconds())

            days = age_seconds // 86400
            hours = (age_seconds % 86400) // 3600
            minutes = (age_seconds % 3600) // 60
            seconds = age_seconds % 60

            if days > 0:
                return f"{days}d"
            elif hours > 0:
                return f"{hours}h"
            elif minutes >= 10:
                return f"{minutes}m"
            elif minutes > 0 or seconds > 0:
                return f"{minutes}m{seconds}s" if minutes > 0 else f"{seconds}s"
            else:
                return "0s"

        # Exact job name match
        for job in all_jobs.items:
            if job.metadata.name == job_info:
                job_details.append({
                    "name": job.metadata.name,
                    "namespace": job.metadata.namespace,
                    "age": format_age(job.metadata.creation_timestamp),
                    # "active": job.status.active if job.status.active else 0,
                    # "succeeded": job.status.succeeded if job.status.succeeded else 0,
                    # "failed": job.status.failed if job.status.failed else 0,
                    "status": get_job_status(job.status),
                    "completions": f"{job.status.succeeded}/{job.spec.completions}" if job.status.succeeded is not None else "0/1"
                })
                return jsonify({"data": job_details}), 200

        # Search by label selector
        label_selector = f"controller-uid={job_info}"
        if namespace == "All namespaces":
            jobs = v1_batch.list_job_for_all_namespaces(label_selector=label_selector)
        else:
            jobs = v1_batch.list_namespaced_job(namespace=namespace, label_selector=label_selector)

        for job in jobs.items:
            job_details.append({
                "name": job.metadata.name,
                "namespace": job.metadata.namespace,
                "age": format_age(job.metadata.creation_timestamp),
                "completions": f"{job.status.succeeded}/{job.spec.completions}" if job.status.succeeded is not None else "0/1",
                "status": get_job_status(job.status)
            })

        # Partial match for job names if no exact or label matches were found
        if not job_details:
            for job in all_jobs.items:
                if job_info in job.metadata.name:
                    job_details.append({
                        "name": job.metadata.name,
                        "namespace": job.metadata.namespace,
                        "age": format_age(job.metadata.creation_timestamp),
                        "completions": f"{job.status.succeeded}/{job.spec.completions}" if job.status.succeeded is not None else "0/1",
                        "status": get_job_status(job.status)
                    })

        if job_details:
            return jsonify({"data": job_details}), 200
        else:
            return jsonify({"error": "No matching jobs found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    

@pb_piescale_kub.route('/get_services', methods=['POST'])
def get_services():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("namespace", "default")
        if namespace == "All namespaces":
            services = v1.list_service_for_all_namespaces()
        else:
            services = v1.list_namespaced_service(namespace)

        services_list = []
        for svc in services.items:
            creation_time = svc.metadata.creation_timestamp
            age = "-"

            if creation_time:
                delta = (datetime.datetime.now(datetime.timezone.utc)) - creation_time
                age = f"{delta.days}d {delta.seconds // 3600}h"
            
            ports = ", ".join(
                f"{p.node_port}:{p.port}/{p.protocol}" if p.node_port else f"{p.port}/{p.protocol}"
                for p in svc.spec.ports
            )
            
            external_ip = ", ".join(
                ingress.ip for ingress in svc.status.load_balancer.ingress
            ) if svc.status.load_balancer and svc.status.load_balancer.ingress else ""
            
            selector = ", ".join(f"{k}={v}" for k, v in svc.spec.selector.items()) if svc.spec.selector else ""
            status = "Active" if svc.spec.cluster_ip else "Inactive"
            services_list.append({
                "name": svc.metadata.name,
                "namespace": svc.metadata.namespace,
                "type": svc.spec.type,
                "cluster_ip": svc.spec.cluster_ip,
                "external_ip": external_ip,
                "ports": ports,
                "selector": selector,
                "age": age,
                "status": status
            })

        return jsonify(services_list), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    

@pb_piescale_kub.route('/get_all_endpoints', methods=['POST'])
def get_all_endpoints():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("namespace", "default")
        endpoints = v1.list_namespaced_endpoints(namespace)
        endpoint_list = []

        for ep in endpoints.items:
            creation_time = ep.metadata.creation_timestamp
            age = (datetime.datetime.now(datetime.timezone.utc) - creation_time).days if creation_time else "Unknown"
            first_endpoint = "<none>"
            if ep.subsets:
                for subset in ep.subsets:
                    if subset.addresses and subset.ports:
                        for address in subset.addresses:
                            for port in subset.ports:
                                first_endpoint = f"{address.ip}:{port.port}"
                                break  
                            if first_endpoint != "<none>":
                                break
                    if first_endpoint != "<none>":
                        break

            ep_info = {
                "Name": ep.metadata.name,
                "Namespace": ep.metadata.namespace,
                "Endpoints": first_endpoint,  
                "Age": f"{age}d"
            }

            endpoint_list.append(ep_info)
        return jsonify(endpoint_list), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_service_accounts', methods=['POST'])
def get_service_accounts():
    try:
        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("namespace", "default")
        accounts = v1.list_namespaced_service_account(namespace=namespace)
        service_accounts = [{"name": sa.metadata.name, "namespace": sa.metadata.namespace} for sa in accounts.items]
        return jsonify({"data": service_accounts}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/list_persistent_volume_claims', methods=['POST'])
def list_persistent_volume_claims():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("namespace", None)

        if namespace == "All namespaces":
            pvc_list = v1.list_persistent_volume_claim_for_all_namespaces()
        else:
            pvc_list = v1.list_namespaced_persistent_volume_claim(namespace)

        pvcs = []
        for pvc in pvc_list.items:
            creation_timestamp = pvc.metadata.creation_timestamp
            age_days = (datetime.datetime.now(creation_timestamp.tzinfo) - creation_timestamp).days if creation_timestamp else ""
            pvc_info = {
                "name": pvc.metadata.name or "",
                "namespace": pvc.metadata.namespace or "",
                "storage_class": pvc.spec.storage_class_name or "",
                "size": pvc.spec.resources.requests.get("storage", ""),
                "pods": ", ".join(pvc.metadata.annotations.get('volume.kubernetes.io/selected-node', "").split()) or "",
                "age": f"{age_days}d" if age_days != "" else "",
                "status": pvc.status.phase or ""
            }
            pvcs.append(pvc_info)

        return jsonify({"pvcs": pvcs}), 200

    except ApiException as e:
        return jsonify({"error": str(e)}), e.status
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        

@pb_piescale_kub.route('/list_persistent_volumes', methods=['POST'])
def list_persistent_volumes():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()

        pv_list = v1.list_persistent_volume()

        pvs = []
        for pv in pv_list.items:
            creation_timestamp = pv.metadata.creation_timestamp
            age_days = (datetime.datetime.now(creation_timestamp.tzinfo) - creation_timestamp).days if creation_timestamp else ""
            pv_info = {
                "name": pv.metadata.name or "",
                "storage_class": pv.spec.storage_class_name or "",
                "capacity": pv.spec.capacity.get("storage", ""),
                "claim": f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}" if pv.spec.claim_ref else "",
                "age": f"{age_days}d" if age_days != "" else "",
                "status": pv.status.phase or ""
            }
            pvs.append(pv_info)

        return jsonify({"pvs": pvs}), 200

    except ApiException as e:
        return jsonify({"error": str(e)}), e.status
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@pb_piescale_kub.route('/list_storage_classes', methods=['POST'])
def list_storage_classes():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        storage_api = client.StorageV1Api()

        storage_classes = storage_api.list_storage_class()
        sc_list = []
        for sc in storage_classes.items:
            creation_timestamp = sc.metadata.creation_timestamp
            age_days = (datetime.datetime.now(creation_timestamp.tzinfo) - creation_timestamp).days if creation_timestamp else ""
            sc_info = {
                "name": sc.metadata.name or "",
                "provisioner": sc.provisioner or "",
                "reclaim_policy": sc.reclaim_policy or "",
                "default": sc.metadata.annotations.get('storageclass.kubernetes.io/is-default-class', "") if sc.metadata.annotations else "",
                "age": f"{age_days}d" if age_days != "" else ""
            }
            sc_list.append(sc_info)

        return jsonify({"storage_classes": sc_list}), 200

    except ApiException as e:
        return jsonify({"error": str(e)}), e.status
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
    

    
@pb_piescale_kub.route('/list_ingress_classes', methods=['POST'])
def list_ingress_classes():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        networking_api = client.NetworkingV1Api()

        ingress_classes = networking_api.list_ingress_class()
        ic_list = []
        for ic in ingress_classes.items:
            creation_timestamp = ic.metadata.creation_timestamp
            age_days = (datetime.datetime.now(creation_timestamp.tzinfo) - creation_timestamp).days if creation_timestamp else ""
            ic_info = {
                "name": ic.metadata.name or "",
                "namespace": ic.metadata.namespace or "",
                "controller": ic.spec.controller or "",
                "api_group": ic.api_version or "",
                "scope": ic.metadata.annotations.get('ingressclass.kubernetes.io/is-default-class', "") if ic.metadata.annotations else "",
                "kind": ic.kind or "",
                "age": f"{age_days}d" if age_days != "" else ""
            }
            ic_list.append(ic_info)

        return jsonify({"ingress_classes": ic_list}), 200

    except ApiException as e:
        return jsonify({"error": str(e)}), e.status
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@pb_piescale_kub.route('/list_network_policies', methods=['POST'])
def list_network_policies():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        networking_api = client.NetworkingV1Api()
        data = request.get_json()
        namespace = data.get('namespace')

        network_policies = networking_api.list_namespaced_network_policy(namespace=namespace)

        np_list = []
        for np in network_policies.items:
            creation_timestamp = np.metadata.creation_timestamp
            age_days = (datetime.datetime.now(creation_timestamp.tzinfo) - creation_timestamp).days if creation_timestamp else ""
            np_info = {
                "name": np.metadata.name or "",
                "namespace": np.metadata.namespace or "",
                "policy_types": ", ".join(np.spec.policy_types) if np.spec.policy_types else "",
                "age": f"{age_days}d" if age_days != "" else ""
            }
            np_list.append(np_info)

        if not np_list:
            np_list.append({
                "name":"",
                "namespace":"",
                "policy_types":"",
                "age":""
            })

        return jsonify({"network_policies": np_list}), 200

    except ApiException as e:
        return jsonify({"error": str(e)}), e.status
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
    

@pb_piescale_kub.route('/get_definitions', methods=['POST'])
def get_definitions():
    try:
        v1 = client.ApiextensionsV1Api()
        crds = v1.list_custom_resource_definition()
        result = [
            {
                "name": crd.metadata.name,
                "group": crd.spec.group,
                "version": crd.spec.versions[0].name if crd.spec.versions else "N/A",
                "scope": crd.spec.scope,
                "age": crd.metadata.creation_timestamp
            }
            for crd in crds.items
        ]
        return jsonify({"items": result, "count": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@pb_piescale_kub.route('/get_events', methods=['POST'])
def get_events():
    try:
        data = request.get_json()
        namespace = data.get("namespace", "default")
        limit = int(data.get("page_size", 10))
        continue_token = data.get("continue_token", None)
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        v1 = client.CoreV1Api()

        # Fetch all events from Kubernetes
        if namespace == "All namespaces":
            events_list = v1.list_event_for_all_namespaces(limit=limit, _continue=continue_token)
        else:
            events_list = v1.list_namespaced_event(namespace, limit=limit, _continue=continue_token)
            
        
        # Get the current UTC time
        current_time = datetime.datetime.now(datetime.timezone.utc)

        # Extract event details from Kubernetes API response
        events = []
        for event in events_list.items:
            creation_time = event.metadata.creation_timestamp

            # Get Age directly from Kubernetes
            age = "Unknown"
            if creation_time:
                delta = current_time - creation_time
                total_seconds = delta.total_seconds()

                if total_seconds < 60:
                    age = f"{int(total_seconds)}s"
                elif total_seconds < 3600:
                    age = f"{int(total_seconds // 60)}m"
                elif total_seconds < 86400:
                    age = f"{int(total_seconds // 3600)}h"
                else:
                    age = f"{int(total_seconds // 86400)}d"

            # Combine kind and name into a single string like OpenLens
            obj_kind = event.involved_object.kind if event.involved_object and event.involved_object.kind else "Unknown"
            obj_name = event.involved_object.name if event.involved_object and event.involved_object.name else "Unknown"
            involved_object = f"{obj_kind}: {obj_name}"

            # Construct event object directly from Kubernetes API response
            event_data = {
                "name": event.metadata.name or "",
                "namespace": event.metadata.namespace or "",
                "message": event.message or "",
                "reason": event.reason or "",
                "type": event.type or "",
                "source": event.source.component if event.source and event.source.component else "",
                "involved_object": involved_object,  #  Single-line format
                "first_seen": event.first_timestamp.isoformat() if event.first_timestamp else "",
                "last_seen": event.last_timestamp.isoformat() if event.last_timestamp else "",
                "count": str(event.count) if event.count is not None else "",  # Convert to string or empty
                "age": age or ""
            }
            events.append(event_data)
        next_continue_token =events_list.metadata._continue if events_list.metadata._continue else None
        response = {"data": events, "continue_token": next_continue_token }
        return jsonify(response), 200

    except client.ApiException as e:
        return jsonify({"error": f"Kubernetes API error: {str(e)}"}), e.status
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    



@pb_piescale_kub.route('/list_ingress', methods=['POST'])
def list_ingress():
    try:
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        networking_api = client.NetworkingV1Api()
        data = request.get_json()
        namespace = data.get("namespace", None)

        if namespace == "All namespaces":
            ingress = networking_api.list_ingress_for_all_namespaces()
        else:
            ingress = networking_api.list_namespaced_ingress(namespace=namespace)
        ic_list = []
        
        for ic in ingress.items:
            creation_timestamp = ic.metadata.creation_timestamp
            age_days = (datetime.datetime.now(creation_timestamp.tzinfo) - creation_timestamp).days if creation_timestamp else ""

            load_balancers = []
            if ic.status.load_balancer and ic.status.load_balancer.ingress:
                for lb in ic.status.load_balancer.ingress:
                    load_balancers.append(lb.ip or lb.hostname or "")

            rules = []
            if ic.spec.rules:
                for rule in ic.spec.rules:
                    rule_info = {
                        "host": rule.host or "",
                        "paths": [
                            {
                                "path": path.path or "",
                                "backend": path.backend.service.name if path.backend and path.backend.service else ""
                            }
                            for path in (rule.http.paths if rule.http else [])
                        ]
                    }
                    rules.append(rule_info)

            ic_info = {
                "name": ic.metadata.name or "",
                "namespace": ic.metadata.namespace or "",
                "age": f"{age_days}d" if age_days != "" else "",
                "load_balancers": load_balancers if load_balancers else "",
                "rules": rules if rules else ""
            }
            ic_list.append(ic_info)

        # If no ingress data is found, return a default empty entry
        if not ic_list:
            ic_list.append({
                "name": "",
                "namespace": "",
                "age": "",
                "load_balancers": "",
                "rules": ""
            })

        return jsonify({"ingress": ic_list}), 200

    except ApiException as e:
        return jsonify({"error": str(e)}), e.status
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500
    

@pb_piescale_kub.route('/get_role_bindings', methods=['POST'])
def get_role_bindings():
    try:
        # Load Kubernetes configuration (optional, if not globally loaded)
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        rbac_v1 = client.RbacAuthorizationV1Api()
        data = request.get_json() or {}
        namespace = data.get("namespace", "default")

        if namespace == "All namespaces":
            bindings = rbac_v1.list_role_binding_for_all_namespaces()
        else:
            bindings = rbac_v1.list_namespaced_role_binding(namespace=namespace)
        role_bindings = []
        for rb in bindings.items:
            creation_time = rb.metadata.creation_timestamp
            age = (datetime.datetime.now(datetime.timezone.utc) - creation_time).days if creation_time else "Unknown"
            age_str = f"{age}d" if isinstance(age, int) else age

            # Extract subjects (Bindings column in Lens)
            subject_names = []
            if rb.subjects:
                for subj in rb.subjects:
                    subject_name = subj.name or ""
                    subject_names.append(subject_name)
            role_bindings.append({
                "Name": rb.metadata.name or "",
                "Namespace": rb.metadata.namespace or "",
                "Bindings": ", ".join(subject_names), 
                "Age": age_str
            })

        return jsonify(role_bindings), 200 

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_roles', methods=['POST'])
def get_roles():

    try:
        # Load Kubernetes configuration (optional, if needed)
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        # Initialize RBAC API client
        rbac_v1 = client.RbacAuthorizationV1Api()
        data = request.get_json() or {} 
        namespace = data.get("namespace", "default")
        if namespace == "All namespaces":
            roles = rbac_v1.list_role_for_all_namespaces()
        else:
            roles = rbac_v1.list_namespaced_role(namespace=namespace)
        role_list = []
        for role in roles.items:
            creation_time = role.metadata.creation_timestamp

            # Calculate age in days
            age = (datetime.datetime.now(datetime.timezone.utc) - creation_time).days if creation_time else "Unknown"
            age_str = f"{age}d" if isinstance(age, int) else age

            # Extract Metadata (Labels & Annotations)
            labels = role.metadata.labels if role.metadata.labels else {}
            annotations = role.metadata.annotations if role.metadata.annotations else {}

            # Extract Role Rules (Resources, Verbs, API Groups)
            rules_list = []
            if role.rules:
                for rule in role.rules:
                    rules_list.append({
                        "Resources": rule.resources if rule.resources else [],
                        "Verbs": rule.verbs if rule.verbs else [],
                        "API Groups": rule.api_groups if rule.api_groups else []
                    })

            # Prepare role data
            role_list.append({
                "Name": role.metadata.name or "",
                "Namespace": role.metadata.namespace or "",
                "Age": age_str,
                "Labels": labels,
                "Annotations": annotations,
                "Rules": rules_list
            })

        if not role_list:
            role_list.append({
                "Name": "",
                "Namespace": "",
                "Age": "",
                "Labels": {},
                "Annotations": {},
                "Rules": []
            })

        return jsonify(role_list), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    

@pb_piescale_kub.route('/get_cluster_roles', methods=['POST'])
def get_cluster_roles():
    try:
        # Load Kubernetes configuration (optional, if not done globally)
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        # Initialize RBAC API client
        rbac_v1 = client.RbacAuthorizationV1Api()

        # Fetch all ClusterRoles
        roles = rbac_v1.list_cluster_role()

        cluster_roles = []
        for role in roles.items:
            creation_time = role.metadata.creation_timestamp

            # Calculate age in days
            age = (datetime.datetime.now(datetime.timezone.utc) - creation_time).days if creation_time else "Unknown"
            age_str = f"{age}d" if isinstance(age, int) else age

            # Extract Labels & Annotations
            labels = role.metadata.labels if role.metadata.labels else {}
            annotations = role.metadata.annotations if role.metadata.annotations else {}

            # Extract rules (resources and verbs)
            rules_list = []
            if role.rules:
                for rule in role.rules:
                    rules_list.append({
                        "Resources": rule.resources if rule.resources else [],
                        "Verbs": rule.verbs if rule.verbs else [],
                        "API Groups": rule.api_groups if rule.api_groups else []
                    })

            # Prepare final dictionary
            cluster_roles.append({
                "Name": role.metadata.name or "",
                "Age": age_str,
                "Labels": labels,
                "Annotations": annotations,
                "Rules": rules_list
            })

        if not cluster_roles:
            cluster_roles.append({
                "Name": "",
                "Age": "",
                "Labels": {},
                "Annotations": {},
                "Rules": []
            })

        return jsonify(cluster_roles), 200  # Returning list of dictionaries

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    

@pb_piescale_kub.route('/get_cluster_role_bindings', methods=['POST'])
def get_cluster_role_bindings():
    try:
        # Load Kubernetes configuration (optional, if not done globally)
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        # Initialize RBAC API client
        rbac_v1 = client.RbacAuthorizationV1Api()

        # Fetch all ClusterRoleBindings
        bindings = rbac_v1.list_cluster_role_binding()

        role_bindings = []
        for rb in bindings.items:
            creation_time = rb.metadata.creation_timestamp

            # Calculate age in days (similar to other APIs)
            age = (datetime.datetime.now(datetime.timezone.utc) - creation_time).days if creation_time else "Unknown"
            age_str = f"{age}d" if isinstance(age, int) else age

            # Extract subjects (Bindings column in Lens)
            subject_names = []
            if rb.subjects:
                for subj in rb.subjects:
                    # Format: kind:name (like User:admin, ServiceAccount:default)
                    subject_name = f"{subj.kind}:{subj.name}"
                    subject_names.append(subject_name)

            # Prepare response structure
            role_bindings.append({
                "Name": rb.metadata.name or "",
                "Bindings": ", ".join(subject_names),  # Comma-separated subjects
                "Age": age_str
            })

        if not role_bindings:
            role_bindings.append({
                "Name":"",
                "Bindings":"",
                "Age":""
            })

        return jsonify(role_bindings), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    


@pb_piescale_kub.route('/get_replicasets', methods=['POST'])
def get_replicasets():
    try:
        # Load Kubernetes config (inside or outside cluster)
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        
        v1_apps = client.AppsV1Api()
        data = request.get_json()

        namespace = data.get("namespace", None)  # Optional

        # Fetch all ReplicaSets at once (No pagination)
        if namespace:
            replica_sets = v1_apps.list_namespaced_replica_set(namespace)
        else:
            replica_sets = v1_apps.list_replica_set_for_all_namespaces()

    
        # Prepare ReplicaSet data
        replicaset_list = [
            {
                "name": rs.metadata.name,
                "namespace": rs.metadata.namespace,
                "desired": rs.spec.replicas,
                "current": rs.status.replicas if rs.status.replicas else 0,
                "ready": rs.status.ready_replicas if rs.status.ready_replicas else 0,
                "age": calculate_age(rs.metadata.creation_timestamp)
            }
            for rs in replica_sets.items
        ]

        if not replicaset_list:
            replicaset_list.append({
                "Name":"",
                "namespace":"",
                "desired":"",
                "current":"",
                "ready":"",
                "Age":""
            })
        # Final response (No `continue_token` since pagination is removed)
        return jsonify({"data": replicaset_list}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/get_driver_pod_logs', methods=['POST'])
def get_driver_pod_logs():
    # Get the payload data
    data = request.get_json()
    run_id = data.get("run_id")
    namespace = data.get("namespace")

    if not run_id or not namespace:
        return jsonify({"status": "error", "message": "run_id and namespace are required in the payload"}), 400

    try:
        # Load Kubernetes config
        try:
            config.load_incluster_config()  # For running inside a cluster
        except:
            config.load_kube_config()  # For running locally

        v1 = client.CoreV1Api()

        # Step 1: Get all pods in the specified namespace with the controller-uid label matching the run_id
        label_selector = f"controller-uid={run_id}"
        pods = v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)

        # Step 2: Find the job-name associated with the run_id
        job_name = None
        for pod in pods.items:
            labels = pod.metadata.labels
            if labels and "job-name" in labels:
                job_name = labels["job-name"]
                break

        if not job_name:
            return jsonify({"status": "error", "message": "Job not found for the given Run ID"}), 404

        # Step 3: Get all pods in the namespace belonging to this job with the spark-role=driver label
        label_selector = f"job-name={job_name},spark-role=driver"
        job_pods = v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)

        # Step 4: Identify the driver pod
        driver_pod = None
        for pod in job_pods.items:
            driver_pod = pod
            break  # Since we expect only one driver pod, we can break after finding it

        if not driver_pod:
            return jsonify({"status": "error", "message": "No driver pod found for the job"}), 404

        # Step 5: Get logs of the driver pod
        logs = v1.read_namespaced_pod_log(
            name=driver_pod.metadata.name,
            namespace=driver_pod.metadata.namespace,
            container=None,  # If you need a specific container, specify it here
            pretty=True,
            timestamps=True,
            tail_lines=1000
        )

        return jsonify({
            "status": "success",
            "job_name": job_name,
            'namespace': namespace,
            "logs": logs
        })
    except client.ApiException as e:
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving logs: {str(e)}",
            'code': e.status
        }), e.status

    except Exception as e:
        return jsonify({"error": str(e)}), 400
@pb_piescale_kub.route('/get_resource_quotas', methods=['POST'])
def get_resource_quotas():
    try:
        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("namespace")

        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        if namespace:
            quotas = v1.list_namespaced_resource_quota(namespace)
        else:
            quotas = v1.list_resource_quota_for_all_namespaces()

        quota_list = [
            {
                "name": quota.metadata.name,
                "namespace": quota.metadata.namespace,
                "age": calculate_age(quota.metadata.creation_timestamp)
            }
            for quota in quotas.items
        ]
        if not quota_list:
            quota_list.append({
                "Name":"",
                "namespace":"",
                "Age":""
            })

        response = {"data":quota_list}
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/get_replication_controllers', methods=['POST'])
def get_replication_controllers():
    try:
        data = request.get_json()
        namespace = data.get("namespace")
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        v1 = client.CoreV1Api()

        if not namespace:
            return jsonify({"error": "Namespace is required"}), 400

        rc_list = v1.list_namespaced_replication_controller(namespace=namespace)
        rc_details = []

        for rc in rc_list.items:
            rc_details.append({
                "name": rc.metadata.name,
                "namespace": rc.metadata.namespace,
                "replicas": rc.status.replicas,
                "desired_replicas": rc.spec.replicas,
                "selector": rc.spec.selector
            })

        if not rc_details:
            rc_details.append({
                "Name":"",
                "namespace":"",
                "desired_replicas":"",
                "selector":"",
                "replicas":""
            })

        return jsonify({"data": rc_details}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_limit_ranges', methods=['POST'])
def get_limit_ranges():
    try:
        v1 = client.CoreV1Api()
        data = request.get_json()
        namespace = data.get("namespace")
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        if namespace:
            limits = v1.list_namespaced_limit_range(namespace)
        else:
            limits = v1.list_limit_range_for_all_namespaces()

        limit_list = [
            {
                "name": limit.metadata.name,
                "namespace": limit.metadata.namespace,
                "range": limit.spec.limits if limit.spec.limits else []
            }
            for limit in limits.items
        ]

        if not limit_list:
            limit_list.append({
                "Name":"",
                "namespace":"",
                "Age":""
            })

        response = {"data": limit_list}
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pb_piescale_kub.route('/get_runtime_classes', methods=['POST'])
def get_runtime_classes():
    try:
        v1 = client.NodeV1Api()
        data = request.get_json()
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        runtime_classes = v1.list_runtime_class()

        runtime_list = [
            {
                "name": runtime.metadata.name,
                "handler": runtime.handler,
                "age": calculate_age(runtime.metadata.creation_timestamp)
            }
            for runtime in runtime_classes.items
        ]

        if not runtime_list:
            runtime_list.append({
                "Name":"",
                "namespace":"",
                "Age":""
            })

        response = {"data": runtime_list}
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/get_pod_metadata', methods=['POST'])
def get_pod_metadata():
    data = request.get_json()
    pod_name = data.get("pod_name")
    namespace = data.get("namespace")

    try:
        try:
            config.load_incluster_config()  
        except:
            config.load_kube_config()  

        v1 = client.CoreV1Api()

        
        pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)

        
        # Labels
        labels = pod.metadata.labels if pod.metadata.labels else {}

        # Node
        node = pod.spec.node_name if pod.spec.node_name else "Not assigned"

        # Service Account
        service_account = pod.spec.service_account_name if pod.spec.service_account_name else "Not specified"

        # Environment Variables
        environment = {}
        for container in pod.spec.containers:
            if container.env:
                for env_var in container.env:
                    # Handle both direct values and values from secrets/configmaps
                    if env_var.value:
                        environment[env_var.name] = env_var.value
                    elif env_var.value_from:
                        # If the value is from a secret or configmap, indicate the source
                        if env_var.value_from.secret_key_ref:
                            environment[env_var.name] = f"Value from Secret: {env_var.value_from.secret_key_ref.name}/{env_var.value_from.secret_key_ref.key}"
                        elif env_var.value_from.config_map_key_ref:
                            environment[env_var.name] = f"Value from ConfigMap: {env_var.value_from.config_map_key_ref.name}/{env_var.value_from.config_map_key_ref.key}"
                        elif env_var.value_from.field_ref:
                            environment[env_var.name] = f"Value from Field: {env_var.value_from.field_ref.field_path}"

        # Step 3: Construct the response
        metadata = {
            "labels": labels,
            "node": node,
            "service_account": service_account,
            "environment": environment
        }

        return jsonify({
            "status": "success",
            "pod_name": pod_name,
            "namespace": namespace,
            "metadata": metadata
        })

		
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@pb_piescale_kub.route('/get_job_metadata', methods=['POST'])
def get_job_metadata():
    data = request.get_json()
    job_name = data.get("job_name")
    namespace = data.get("namespace")

    # Validate the payload
    if not job_name or not namespace:
        return jsonify({"status": "error", "message": "job_name and namespace are required in the payload"}), 400

    try:
        
        try:
            config.load_incluster_config()  
        except:
            config.load_kube_config()  

        # Initialize the Kubernetes BatchV1Api client for Jobs
        batch_v1 = client.BatchV1Api()
        # Initialize CoreV1Api client for pod-related information
        core_v1 = client.CoreV1Api()

        # Step 1: Fetch the Job details using the job name and namespace
        job = batch_v1.read_namespaced_job(name=job_name, namespace=namespace)

        # Step 2: Extract the required metadata
        # Labels
        labels = job.metadata.labels if job.metadata.labels else {}

        # Images (from the pod template in the Job spec)
        images = []
        if job.spec.template.spec.containers:
            for container in job.spec.template.spec.containers:
                images.append(container.image)

        # Status
        status = "Unknown"
        if job.status.conditions:
            for condition in job.status.conditions:
                if condition.status == "True":
                    status = condition.type  # e.g., "Complete" or "Failed"
                    break
        elif job.status.active:
            status = "Running"
        elif job.status.succeeded:
            status = "Complete"
        elif job.status.failed:
            status = "Failed"

        # Completions and Parallelism
        completions = job.spec.completions if job.spec.completions else 0
        parallelism = job.spec.parallelism if job.spec.parallelism else 0

        # Pod Status (find pods associated with this job)
        pod_status = "No pods found"
        label_selector = f"job-name={job_name}"
        pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        if pods.items:
            pod_status = []
            for pod in pods.items:
                pod_phase = pod.status.phase if pod.status.phase else "Unknown"
                pod_status.append({
                    "pod_name": pod.metadata.name,
                    "phase": pod_phase
                })

        # Step 3: Construct the response
        metadata = {
            "labels": labels,
            "images": images,
            "status": status,
            "completions": completions,
            "parallelism": parallelism,
            "pod_status": pod_status
        }

        return jsonify({
            "status": "success",
            "job_name": job_name,
            "namespace": namespace,
            "metadata": metadata
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
def get_overview():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch_v1 = client.BatchV1Api()

    data = request.json
    namespace = data.get("namespace", None)

    if namespace == "All namespaces":
        pods = v1.list_pod_for_all_namespaces().items
        deployments = apps_v1.list_deployment_for_all_namespaces().items
        daemon_sets = apps_v1.list_daemon_set_for_all_namespaces().items
        stateful_sets = apps_v1.list_stateful_set_for_all_namespaces().items
        replica_sets = apps_v1.list_replica_set_for_all_namespaces().items
        jobs = batch_v1.list_job_for_all_namespaces().items
        cron_jobs = batch_v1.list_cron_job_for_all_namespaces().items
    else:
        pods = v1.list_namespaced_pod(namespace).items
        deployments = apps_v1.list_namespaced_deployment(namespace).items
        daemon_sets = apps_v1.list_namespaced_daemon_set(namespace).items
        stateful_sets = apps_v1.list_namespaced_stateful_set(namespace).items
        replica_sets = apps_v1.list_namespaced_replica_set(namespace).items
        jobs = batch_v1.list_namespaced_job(namespace).items
        cron_jobs = batch_v1.list_namespaced_cron_job(namespace).items

    # Pods
    pods_count = len(pods)
    pods_succeeded = sum(1 for pod in pods if pod.status.phase == "Succeeded")
    pods_running = sum(1 for pod in pods if pod.status.phase == "Running")
    pods_pending = sum(1 for pod in pods if pod.status.phase == "Pending")
    pods_failed = sum(1 for pod in pods if pod.status.phase == "Failed")

    # Deployments
    deployments_count = len(deployments)
    deployments_running = sum(1 for d in deployments if d.status.available_replicas == d.status.replicas)

    # DaemonSets
    daemon_sets_count = len(daemon_sets)
    daemon_sets_running = sum(1 for d in daemon_sets if d.status.number_ready == d.status.desired_number_scheduled)
    daemon_sets_pending = daemon_sets_count - daemon_sets_running

    # StatefulSets
    stateful_sets_count = len(stateful_sets)
    stateful_sets_running = sum(1 for s in stateful_sets if s.status.ready_replicas == s.status.replicas)

    # ReplicaSets
    replica_sets_count = len(replica_sets)
    replica_sets_running = sum(1 for r in replica_sets if r.status.ready_replicas == r.status.replicas)

    # Jobs
    job_count = len(jobs)
    jobs_succeeded = sum(job.status.succeeded or 0 for job in jobs)
    jobs_running = sum(job.status.active or 0 for job in jobs)
    jobs_failed = sum(job.status.failed or 0 for job in jobs)

    # CronJobs
    cron_job_count = len(cron_jobs)
    cron_jobs_scheduled = sum(1 for cj in cron_jobs if not cj.spec.suspend)
    cron_jobs_suspended = sum(1 for cj in cron_jobs if cj.spec.suspend)

    # Construct response
    response = {
        "Pods": {
            "total": pods_count,
            "Succeeded": pods_succeeded,
            "Running": pods_running,
            "Pending": pods_pending,
            "Failed": pods_failed
        },
        "Deployments": {
            "total": deployments_count,
            "Running": deployments_running
        },
        "DaemonSets": {
            "total": daemon_sets_count,
            "Running": daemon_sets_running,
            "Pending": daemon_sets_pending
        },
        "StatefulSets": {
            "total": stateful_sets_count,
            "Running": stateful_sets_running
        },
        "ReplicaSets": {
            "total": replica_sets_count,
            "Running": replica_sets_running
        },
        "Jobs": {
            "total": job_count,
            "Succeeded": jobs_succeeded,
            "Running": jobs_running,
            "Failed": jobs_failed
        },
        "CronJobs": {
            "total": cron_job_count,
            "Scheduled": cron_jobs_scheduled,
            "Suspended": cron_jobs_suspended
        }
    }
    return response

@pb_piescale_kub.route('/get_overview', methods=['POST'])
def get_overview_api():
    response = get_overview()
    return jsonify(response)
    
@pb_piescale_kub.route("/delete_job", methods=["POST"])
def delete_job():
    
    # Delete a single Kubernetes Job by name and namespace.
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    data = request.json
    job_name = data.get("job_name")
    namespace = data.get("namespace")

    if not job_name or not namespace:
        return jsonify({"error": "job_name and namespace are required"}), 400

    try:
        batch_v1.delete_namespaced_job(
            name=job_name, 
            namespace=namespace, 
            body=client.V1DeleteOptions(propagation_policy="Foreground")
        )
        return jsonify({"message": f"Job {job_name} deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@pb_piescale_kub.route("/delete_multiple_jobs", methods=["POST"])
def delete_multiple_jobs():
  
    # Delete multiple Kubernetes Jobs by names and namespace.
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    data = request.json
    job_names = data.get("job_names", [])
    namespace = data.get("namespace")

    if not job_names or not namespace:
        return jsonify({"error": "job_names (list) and namespace are required"}), 400

    deleted_jobs = []
    failed_jobs = []

    for job_name in job_names:
        try:
            batch_v1.delete_namespaced_job(
                name=job_name, 
                namespace=namespace, 
                body=client.V1DeleteOptions(propagation_policy="Foreground")
            )
            deleted_jobs.append(job_name)
        except Exception as e:
            failed_jobs.append({"job_name": job_name, "error": str(e)})

    return jsonify({"deleted_jobs": deleted_jobs, "failed_jobs": failed_jobs}), 200

@pb_piescale_kub.route('/schedule_emr_job', methods=['POST'])
def schedule_emr_job():
    try:
        data = request.get_json()
        script_path = data.get('script_path')
        job_name = data.get('job_name')
        db_prefix = data.get('db_prefix')
        job_id = data.get('job_id')
        schedule = data.get('cron_syntax')
        job_parameter = data.get('job_parameter')
        namespace = os.getenv("namespace", 'petrahub')

        emr_image = os.getenv('emr_image')
        base_url = os.getenv("flask_api_url")
        sa = os.getenv("service_account_name")

        step_name = f"{job_name}"
        args = [str(arg) for arg in [job_id, step_name, script_path, db_prefix, base_url, job_parameter]]

        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        labels = {"custom-cron-job": "true"}
        command = ["python3", "schedule_job.py"]
        
        cron_job = client.V1CronJob(
            metadata=client.V1ObjectMeta(
                name=f"{job_name}",
                labels=labels  # Add labels here
            ),
            spec=client.V1CronJobSpec(
                schedule=schedule,
                job_template=client.V1JobTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels=labels  # Add labels to the job template metadata
                    ),
                    spec=client.V1JobSpec(
                        template=client.V1PodTemplateSpec(
                            metadata=client.V1ObjectMeta(
                                labels=labels  # Add labels to the pod template metadata
                            ),
                            spec=client.V1PodSpec(
                                service_account_name=sa,
                                containers=[
                                    client.V1Container(
                                        name="cron-job-container",
                                        image=emr_image,
                                        command=command,
                                        args=args
                                    )
                                ],
                                restart_policy="OnFailure",
                            )
                        )
                    )
                )
            )
        )
        
        # Create the CronJob
        v1_batch = client.BatchV1Api()
        api_response = v1_batch.create_namespaced_cron_job(
            namespace=namespace,
            body=cron_job
        )
        print("api_response")
        print(api_response)
        return {"response": "success"}

    except Exception as e:
        return {"error": f"An unexpected error occurred: {str(e)}"}