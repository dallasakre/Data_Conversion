__author__ = 'dja410'

import boto.ec2
import sys


credentials = {
  'aws_access_key_id': 'key',
  'aws_secret_access_key': 'secret',
  }


def stop_instance(instance_nm):
    conn = boto.ec2.connect_to_region("us-east-1", **credentials)

    try:

        reservations = conn.get_all_instances(instance_ids=instance_nm)
        instance = reservations[0].instances[0]
        state = instance.state
        if state == 'running':
            instance.stop()

    except Exception, e:

        error = "Error: %s" % str(e)
        print(error)
        sys.exit(0)


ec2instances = [('i-a75cef72', 'small'),
                ('i-d0018505', 'small'),
                ('i-d4018501', 'large'),
                ('i-d6018503', 'large'),
                ('i-d7018502', 'large')]


for instance_nm, size in ec2instances:
    stop_instance(instance_nm)


