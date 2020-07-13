from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.pod import Port
from airflow import models
import datetime

secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
volume_mount = VolumeMount('test-volume',
                            mount_path='/root/mount_file',
                            sub_path=None,
                            read_only=True)
port = Port('http', 80)
configmaps = ['test-configmap-1', 'test-configmap-2']

volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'test-volume'
      }
    }
volume = Volume(name='test-volume', configs=volume_config)

affinity = {
    'nodeAffinity': {
      'preferredDuringSchedulingIgnoredDuringExecution': [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": {
              "key": "disktype",
              "operator": "In",
              "values": ["ssd"]
            }
          }
        }
      ]
    },
    "podAffinity": {
      "requiredDuringSchedulingIgnoredDuringExecution": [
        {
          "labelSelector": {
            "matchExpressions": [
              {
                "key": "security",
                "operator": "In",
                "values": ["S1"]
              }
            ]
          },
          "topologyKey": "failure-domain.beta.kubernetes.io/zone"
        }
      ]
    },
    "podAntiAffinity": {
      "requiredDuringSchedulingIgnoredDuringExecution": [
        {
          "labelSelector": {
            "matchExpressions": [
              {
                "key": "security",
                "operator": "In",
                "values": ["S2"]
              }
            ]
          },
          "topologyKey": "kubernetes.io/hostname"
        }
      ]
    }
}

tolerations = [
    {
        'key': "key",
        'operator': 'Equal',
        'value': 'value'
     }
]
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# If a Pod fails to launch, or has an error occur in the container, Airflow
# will show the task as failed, as well as contain all of the task logs
# required to debug.
with models.DAG(
        dag_id='composer_sample_kubernetes_pod',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:
	k1 = KubernetesPodOperator(namespace='airflowsp',
	                          image="ubuntu:16.04",
	                          cmds=["bash", "-cx"],
	                          arguments=["echo", "10"],
	                          labels={"foo": "bar"},
	                          #secrets=[secret_file, secret_env, secret_all_keys],
	                          #ports=[port],
	                          #volumes=[volume],
	                          #volume_mounts=[volume_mount],
	                          name="test1",
	                          task_id="task1",
	                          #affinity=affinity,
	                          is_delete_operator_pod=True,
	                          #hostnetwork=False,
	                          #tolerations=tolerations,
	                          #configmaps=configmaps
	                          )

	k2 = KubernetesPodOperator(namespace='airflowsp',
	                          image="ubuntu:16.04",
	                          cmds=["bash", "-cx"],
	                          arguments=["echo", "10"],
	                          labels={"foo": "bar"},
	                          #secrets=[secret_file, secret_env, secret_all_keys],
	                          #ports=[port],
	                          #volumes=[volume],
	                          #volume_mounts=[volume_mount],
	                          name="test2",
	                          task_id="task2",
	                          #affinity=affinity,
	                          is_delete_operator_pod=True,
	                          #hostnetwork=False,
	                          #tolerations=tolerations,
	                          #configmaps=configmaps
	                          )

	k3 = KubernetesPodOperator(namespace='airflowsp',
	                          image="ubuntu:16.04",
	                          cmds=["bash", "-cx"],
	                          arguments=["echo", "10"],
	                          labels={"foo": "bar"},
	                          #secrets=[secret_file, secret_env, secret_all_keys],
	                          #ports=[port],
	                          #volumes=[volume],
	                          #volume_mounts=[volume_mount],
	                          name="test3",
	                          task_id="task3",
	                          #affinity=affinity,
	                          is_delete_operator_pod=True,
	                          #hostnetwork=False,
	                          #tolerations=tolerations,
	                          #configmaps=configmaps
	                          )


	k4 = KubernetesPodOperator(namespace='airflowsp',
	                          image="ubuntu:16.04",
	                          cmds=["bash", "-cx"],
	                          arguments=["echo", "10"],
	                          labels={"foo": "bar"},
	                          #secrets=[secret_file, secret_env, secret_all_keys],
	                          #ports=[port],
	                          #volumes=[volume],
	                          #volume_mounts=[volume_mount],
	                          name="test4",
	                          task_id="task4",
	                          #affinity=affinity,
	                          is_delete_operator_pod=True,
	                          #hostnetwork=False,
	                          #tolerations=tolerations,
	                          #configmaps=configmaps
	                          )

k1 >> [k2,k3] >> k4
