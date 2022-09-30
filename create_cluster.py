from distutils.command.clean import clean
import pandas as pd
import boto3
import json
import configparser
import sys


def setup():
    """
    Sets up the cluster by doing the following:
    1. Create a new role to manage redshift.
    2. Set up a policity that gives permission to read S3 buckets.
    """
    config = createConfig()
    clients = createClients(config)

    # Set up the identity and roles.
    createIamRole(config, clients)
    createPolicy(config, clients)
    arn = getIAMRoleARN(config, clients)
    print("ARN: {}".format(arn))

    # Set up the cluster.
    setupCluster(config, clients, arn)

    props = getClusterProperties(config, clients)
    setupIngress(config, clients, props)


def info():
    """
    Cleans up the cluster and roles created using the setup() function.
    Verify the result by hand in the dashboard!
    """
    config = createConfig()
    clients = createClients(config)
    clusterInfo = getClusterProperties(config, clients)
    print("""Status  : {}
Endpoint: {}
Id      : {}
DBName  : {}
Nodes   : {}
Vpc Id  : {}""".format(clusterInfo['ClusterStatus'], clusterInfo['Endpoint'], clusterInfo['ClusterIdentifier'],
                       clusterInfo['DBName'],
                       clusterInfo['NumberOfNodes'], clusterInfo['VpcId']))


def cleanup():
    """
    Cleans up the cluster and roles created using the setup() function.
    Verify the result by hand in the dashboard!
    """
    config = createConfig()
    clients = createClients(config)
    props = getClusterProperties(config, clients)
    deleteCluster(config, clients)
    deleteRolePolicy(config, clients)
    deleteIamRole(config, clients)
    deleteIngress(config, clients, props)


def createConfig():
    """
    Reads the variables form the configuration file dwh.cfg and returns a dictionary.
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return {
        'KEY': config.get('AWS', 'KEY'),
        'SECRET': config.get('AWS', 'SECRET'),
        'DWH_IAM_ROLE_NAME': config.get("DWH", "DWH_IAM_ROLE_NAME"),

        'DWH_CLUSTER_TYPE': config.get("DWH", "DWH_CLUSTER_TYPE"),
        'DWH_NODE_TYPE': config.get("DWH", "DWH_NODE_TYPE"),
        'DWH_NUM_NODES': config.get("DWH", "DWH_NUM_NODES"),
        'DWH_DB': config.get("DWH", "DWH_DB"),
        'DWH_CLUSTER_IDENTIFIER': config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
        'DWH_DB_USER': config.get("DWH", "DWH_DB_USER"),
        'DWH_DB_PASSWORD': config.get("DWH", "DWH_DB_PASSWORD"),
        'DWH_PORT': config.get("DWH", "DWH_PORT"),

        'ARN': config.get("IAM_ROLE", "ARN")
    }


def createClients(config):
    """
    Creates clients for all the resources we will need to install our cluster. 
    """

    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=config['KEY'],
                         aws_secret_access_key=config['SECRET']
                         )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=config['KEY'],
                        aws_secret_access_key=config['SECRET']
                        )

    iam = boto3.client('iam', aws_access_key_id=config['KEY'],
                       aws_secret_access_key=config['SECRET'],
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=config['KEY'],
                            aws_secret_access_key=config['SECRET']
                            )

    return {
        'ec2': ec2, 's3': s3, 'iam': iam, 'redshift': redshift
    }


def createIamRole(config, clients):
    """
    Creates an IAM role to manage redshift.
    """
    try:
        dwhRole = clients['iam'].create_role(
            Path='/',
            RoleName=config['DWH_IAM_ROLE_NAME'],
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}))
    except Exception as e:
        print("Error occured when trying to create the role {}.".format(
            config['DWH_IAM_ROLE_NAME']))
        print(e)


def deleteIamRole(config, clients):
    try:
        clients['iam'].delete_role(RoleName=config['DWH_IAM_ROLE_NAME'])
    except Exception as e:
        print("Exception occurred when trying to delete the IAM role.")
        print(e)


def createPolicy(config, clients):
    """
    Creates a policity that gives the role read-only access to s3 buckets.
    """
    try:
        clients['iam'].attach_role_policy(RoleName=config['DWH_IAM_ROLE_NAME'],
                                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                          )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print("Exception occurred when trying to create a policy.")
        print(e)


def deleteRolePolicy(config, clients):
    """
    Deletes the firewall policy that gives public access to the cluster.
    """
    try:
        clients['iam'].detach_role_policy(
            RoleName=config['DWH_IAM_ROLE_NAME'], PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as e:
        print("Exception occurred when trying to delete the role policy.")
        print(e)


def getIAMRoleARN(config, clients):
    """
    Returns the role arn for the  role we created in createPolicy().
    """
    roleArn = clients['iam'].get_role(
        RoleName=config['DWH_IAM_ROLE_NAME'])['Role']['Arn']
    return roleArn


def setupCluster(config, clients, roleArn):
    """
    Creates a Redshift cluster.
    """
    try:
        response = clients['redshift'].create_cluster(
            # HW
            ClusterType=config['DWH_CLUSTER_TYPE'],
            NodeType=config['DWH_NODE_TYPE'],
            NumberOfNodes=int(config['DWH_NUM_NODES']),

            # Identifiers & Credentials
            DBName=config['DWH_DB'],
            ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['DWH_DB_USER'],
            MasterUserPassword=config['DWH_DB_PASSWORD'],

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print("Exception creating cluster.")
        print(e)


def getClusterProperties(config, clients):
    """
    Gets the live properties of the cluster we just created.
    """
    return clients['redshift'].describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]


def setupIngress(config, clients, clusterProperties):
    """
    Exposes the cluster to the internet on the port defined in dwh.cfg.
    """
    try:
        vpc = clients['ec2'].Vpc(id=clusterProperties['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['DWH_PORT']),
            ToPort=int(config['DWH_PORT'])
        )
    except Exception as e:
        print("An exception occurred trying to setup the ingress rule for the cluster.")
        print(e)


def deleteIngress(config, clients, clusterProperties):
    """
    Remove the firewall rule that exposes the cluster to the internet.
    """
    try:
        vpc = clients['ec2'].Vpc(id=clusterProperties['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.revoke_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['DWH_PORT']),
            ToPort=int(config['DWH_PORT'])
        )
    except Exception as e:
        print("An exception occurred trying to setup the ingress rule for the cluster.")
        print(e)


def deleteCluster(config, clients):
    try:
        clients['redshift'].delete_cluster(
            ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'], SkipFinalClusterSnapshot=True)
    except Exception as e:
        print("An error occured trying to delete the cluster.")
        print(e)


def main():
    flag = sys.argv[1]
    if flag == "create":
        setup()
    elif flag == "cleanup":
        cleanup()
    elif flag == "info":
        info()
    else:
        print("Invalid flag. Use 'create' or 'cleanup'.")


if __name__ == "__main__":
    main()
