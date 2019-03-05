"""
  Script Name:  AWSResourceAutoTaggingLambdaFunction.py
  Purpose:  This script gets executed with each event (i.e. triggered by cloudwatdch event rule) for each resource creation of EC2 or S3 or RDS type and tags them with Moody's standard tag values.The tags also include information regarding the user who created the resource.
  Requirement: For this script to work, CloudWatch must have been enabled, and cloudwatdch event rule has to be set up for the particular APIs of creation of EC2/S3/RDS resource type. Also this lambda function requires permissions as read permission for EC2, S3, RDS, tagging permission for EC2, S3, RDS, IAM Read permission (to fetch alias of the AWS account)
  Author:  Lipsa Parida - MIT
"""
from __future__ import print_function
import sys
import traceback
import json
import boto3
import logging
import time
import datetime
import string
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    #logger.info('Event: ' + str(event))
    #print('Received event: ' + json.dumps(event, indent=2))
 
    ids = []
 
    try:
        region = event['region']
        detail = event['detail']
        eventname = detail['eventName']
        arn = detail['userIdentity']['arn']
        principal = detail['userIdentity']['principalId']
        userType = detail['userIdentity']['type']
        #lob = 'MIT'
        #env = 'MIT_SS_SBX'
        iam_client = boto3.client('iam')
        #Fetch the account Alias
        alias = iam_client.list_account_aliases().get('AccountAliases', [])
        for i in alias:
            account_alias = i
        logger.info(alias)
        logger.info(account_alias)
        # str1 is the account_alias without ma-/moodys-
        if "ma-" in account_alias:
            str1 = string.replace(account_alias, "ma-", "")
        elif "mit-" in account_alias:
            str1 = account_alias
        elif "moodys-" in account_alias:
            str1 = string.replace(account_alias, "moodys-", "")
        else:
            env = 'Dev_Test'
        #split str1 by - and it will retutrn a list of strings. Out of which the last string is the env and rest all together will form the lob
        #list1 is the list of strings present in str1 after split by -
        list = str1.split('-')
        env1 = (list[-1])
        env = env1.upper()
        backup = ''
        if env == 'PRD':
            backup = 'monthly'
        elif env == 'NPRD':
            backup = ''
        elif env == 'SBX':
            backup = ''
        else:
            backup = ''
        logger.info(env)
        list1 = list[:-1]
        logger.info(list1)
        lob1 = ('_'.join(list1))
        lob = lob1.upper()
        logger.info(lob)
        application = ''
        business_service = ''
        patch_level = 'MIT'
        
        monitoring_strategy = 'Persistent'
        rdsArn = ''
        if userType == 'IAMUser':
            user = detail['userIdentity']['userName']
            rolename = "IAM_USER"
 
        else:
            user = principal.split(':')[1]
            rolename = detail['userIdentity']['sessionContext']['sessionIssuer']['userName']
            
        logger.info('principalId: ' + str(principal))
        logger.info('region: ' + str(region))
        logger.info('eventName: ' + str(eventname))
        logger.info('detail: ' + str(detail))

        """
        if not detail['responseElements']:
            logger.warning('No responseElements found')
            if detail['errorCode']:
                logger.error('errorCode: ' + detail['errorCode'])
            if detail['errorMessage']:
                logger.error('errorMessage: ' + detail['errorMessage'])
            return False
        """
        
        ec2 = boto3.resource('ec2')
        client = boto3.client('ec2',region_name=region)
        rds_client = boto3.client('rds',region_name=region)
        s3_client = boto3.client('s3',region_name=region)
        
        if eventname == 'CreateVolume':
            ids.append(detail['responseElements']['volumeId'])
            logger.info(ids)
            for id in ids:
                volumes_details = client.describe_volumes(VolumeIds=[id]).get('Volumes', [])
                for vol in volumes_details:
				    try:
				        vol['Tags']
				        for tags in vol['Tags']:
				            key = tags['Key']
				            if key == 'Application':
				                application = tags['Value']
				            elif key == 'Business_Service':
				                business_service = tags['Value']
				            elif key == 'Patch_Level':
				                patch_level = tags['Value']
				            elif key == 'Backup':
				                backup = tags['Value']
				            elif key == 'Monitoring_Strategy':
				                monitoring_strategy = tags['Value']
				    except KeyError:
				        logger.info('No tags provided by user while resource creation')				
 
        elif eventname == 'RunInstances':
            items = detail['responseElements']['instancesSet']['items']
            for item in items:
                ids.append(item['instanceId'])
            logger.info(ids)
            logger.info('number of instances: ' + str(len(ids)))
            
 #Creates an iterable of all Instance resources in the collection.
            base = ec2.instances.filter(InstanceIds=ids)
            
            for id in ids:
				reservations = client.describe_instances(InstanceIds=[id]).get('Reservations', [])
				instances_details = sum([[i for i in r['Instances']] for r in reservations], [])
				for ins in instances_details:
				    try:
				        ins['Tags']
				        for tags in ins['Tags']:
				            key = tags['Key']
				            if key == 'Application':
				                application = tags['Value']
				            elif key == 'Business_Service':
				                business_service = tags['Value']
				            elif key == 'Patch_Level':
				                patch_level = tags['Value']
				            elif key == 'Backup':
				                backup = tags['Value']
				            elif key == 'Monitoring_Strategy':
				                monitoring_strategy = tags['Value']
				    except KeyError:
				        logger.info('No tags provided by user while resource creation')
 
            #loop through the instances and gather ids ebs volumes and inis
            for instance in base:
                for vol in instance.volumes.all():
                    ids.append(vol.id)
                    logger.info(ids)
                for eni in instance.network_interfaces:
                    ids.append(eni.id)
                    logger.info(ids)
					
        elif eventname == 'RequestSpotInstances':
            items = detail['responseElements']['spotInstanceRequestSet']['items']
            for item in items:
                sirids.append(item['spotInstanceRequestId'])
            logger.info(sirids)
            logger.info('number of spot instances: ' + str(len(sirids)))
            
            sirs = client.describe_spot_instance_requests(SpotInstanceRequestIds=sirids)
            sirs = sirs['SpotInstanceRequests']
            logger.info(sirs)
            
            for sir in sirs:
                ids.append(sir['InstanceId'])
			
 #Creates an iterable of all Instance resources in the collection.
            base = ec2.instances.filter(InstanceIds=ids)
            
            for id in ids:
				reservations = client.describe_instances(InstanceIds=[id]).get('Reservations', [])
				instances_details = sum([[i for i in r['Instances']] for r in reservations], [])
				for ins in instances_details:
				    try:
				        ins['Tags']
				        for tags in ins['Tags']:
				            key = tags['Key']
				            if key == 'Application':
				                application = tags['Value']
				            elif key == 'Business_Service':
				                business_service = tags['Value']
				            elif key == 'Patch_Level':
				                patch_level = tags['Value']
				            elif key == 'Backup':
				                backup = tags['Value']
				            elif key == 'Monitoring_Strategy':
				                monitoring_strategy = tags['Value']
				    except KeyError:
				        logger.info('No tags provided by user while resource creation')
 
            #loop through the instances and gather ids ebs volumes and inis
            for instance in base:
                for vol in instance.volumes.all():
                    ids.append(vol.id)
                    logger.info(ids)
                for eni in instance.network_interfaces:
                    ids.append(eni.id)
                    logger.info(ids)					
 
        elif eventname == 'CreateImage':
            ids.append(detail['responseElements']['imageId'])
            logger.info(ids)
            
            str1 = json.dumps(detail['responseElements']['imageId'])
            i_len = len(str1)
            image_ids = str1[1:(i_len-1)]
            
            images = client.describe_images(ImageIds=[image_ids]).get('Images',[])

            snapshots = sum(
                [
                    [i for i in r['BlockDeviceMappings']]
                    for r in images
                ], [])
            for snap in snapshots:
                ids.append(snap['Ebs']['SnapshotId'])
                logger.info(ids)
                
        elif eventname == 'CreateSnapshot':
            ids.append(detail['responseElements']['snapshotId'])
            logger.info(ids)
        
        elif eventname == 'CreateInternetGateway':
            ids.append(detail['responseElements']['internetGatewayId'])
            logger.info(ids)
            
        elif eventname == 'CreateSecurityGroup':
            ids.append(detail['responseElements']['groupId'])
            logger.info(ids) 
 
        elif eventname == 'CreateVpc':
            ids.append(detail['responseElements']['vpcId'])
            logger.info(ids) 
            
        elif eventname == 'CreateSubnet':
            ids.append(detail['responseElements']['subnetId'])
            logger.info(ids)
            
        elif eventname == 'CreateRouteTable':
            ids.append(detail['responseElements']['routeTableId'])
            logger.info(ids)
 
        elif eventname == 'CreateNetworkAcl':
            ids.append(detail['responseElements']['networkAclId'])
            logger.info(ids) 
            
        elif eventname == 'CreateNatGateway':
            ids.append(detail['responseElements']['natGatewayId'])
            logger.info(ids)
            
        elif eventname == 'CreateCustomerGateway':
            ids.append(detail['responseElements']['customerGatewayId'])
            logger.info(ids)   
            
        elif eventname == 'CreateVpnGateway':
            ids.append(detail['responseElements']['vpnGatewayId'])
            logger.info(ids)   
            
        elif eventname == 'CreateVpnConnection':
            ids.append(detail['responseElements']['vpnConnectionId'])
            logger.info(ids)
            
        elif eventname == 'CreateDBInstance':
            db_instance_ARN = (detail['responseElements']['dBInstanceArn'])
            #db_instance_name = (detail['responseElements']['dBName'])
            rdsArn = db_instance_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')
            
        elif eventname == 'CreateDBSnapshot':
            db_snapshot_ARN = (detail['responseElements']['dBSnapshotArn'])
            logger.info(db_snapshot_ARN)
            rdsArn = db_snapshot_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateDBCluster':
            db_cluster_ARN = (detail['responseElements']['dBClusterArn'])
            logger.info(db_cluster_ARN)
            rdsArn = db_cluster_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')          
            
        elif eventname == 'CreateDBClusterParameterGroup':
            db_cluster_parameterGroup_ARN = (detail['responseElements']['dBClusterParameterGroupArn'])
            logger.info(db_cluster_parameterGroup_ARN)
            rdsArn = db_cluster_parameterGroup_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateDBClusterSnapshot':
            db_cluster_snapshot_ARN = (detail['responseElements']['dBClusterSnapshotArn'])
            logger.info(db_cluster_snapshot_ARN)
            rdsArn = db_cluster_snapshot_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateDBInstanceReadReplica':
            db_instance_rr_ARN = (detail['responseElements']['dBInstanceArn'])
            logger.info(db_instance_rr_ARN)
            rdsArn = db_instance_rr_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateDBParameterGroup':
            db_ParameterGroup_ARN = (detail['responseElements']['dBParameterGroupArn'])
            logger.info(db_ParameterGroup_ARN)
            rdsArn = db_ParameterGroup_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateDBSecurityGroup':
            db_SecurityGroup_ARN = (detail['responseElements']['dBSecurityGroupArn'])
            logger.info(db_SecurityGroup_ARN)
            rdsArn = db_SecurityGroup_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateDBSubnetGroup':
            db_SubnetGroup_ARN = (detail['responseElements']['dBSubnetGroupArn'])
            logger.info(db_SubnetGroup_ARN)
            rdsArn = db_SubnetGroup_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
            
        elif eventname == 'CreateEventSubscription':
            db_eventSubscription_ARN = (detail['responseElements']['eventSubscriptionArn'])
            logger.info(db_eventSubscription_ARN)
            rdsArn = db_eventSubscription_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')            
         
        elif eventname == 'CreateOptionGroup':
            db_optionGroup_ARN = (detail['responseElements']['optionGroupArn'])
            logger.info(db_optionGroup_ARN)
            rdsArn = db_optionGroup_ARN
            logger.info(rdsArn)
            tag_list = rds_client.list_tags_for_resource(ResourceName = rdsArn).get('TagList',[])
            try:
                tag_list
                for tags in tag_list:
                    key = tags['Key']
                    if key == 'Application':
                        application = tags['Value']
                    elif key == 'Business_Service':
                        business_service = tags['Value']
                    elif key == 'Patch_Level':
                        patch_level = tags['Value']
                    elif key == 'Backup':
                        backup = tags['Value']
                    elif key == 'Monitoring_Strategy':
                        monitoring_strategy = tags['Value']
                    logger.info(tags)
            except KeyError:
                logger.info('No tags provided by user while resource creation')
                
        elif eventname == 'CreateBucket':
            bucket_name = (detail['requestParameters']['bucketName'])
            logger.info(bucket_name)
            print('Tagging resource ' + bucket_name)
            s3_client.put_bucket_tagging(Bucket = bucket_name, Tagging={'TagSet': [{'Key': 'Provisioned_By', 'Value': user}, {'Key': 'LOB', 'Value': lob}, {'Key': 'Role', 'Value': rolename}, {'Key': 'Environment', 'Value': env}, {'Key': 'Application', 'Value': application}, {'Key': 'Business_Service', 'Value': business_service}, {'Key': 'Patch_Level', 'Value': patch_level}, {'Key': 'Backup', 'Value': backup}, {'Key': 'Monitoring_Strategy', 'Value': monitoring_strategy}]})

        else:
            logger.warning('Not supported action')
 
        if ids:
            for resourceid in ids:
                print('Tagging resource ' + resourceid)
            ec2.create_tags(Resources=ids, Tags=[{'Key': 'Provisioned_By', 'Value': user}, {'Key': 'LOB', 'Value': lob}, {'Key': 'Role', 'Value': rolename}, {'Key': 'Environment', 'Value': env}, {'Key': 'Application', 'Value': application}, {'Key': 'Business_Service', 'Value': business_service}, {'Key': 'Patch_Level', 'Value': patch_level}, {'Key': 'Backup', 'Value': backup}, {'Key': 'Monitoring_Strategy', 'Value': monitoring_strategy}])
        
        if rdsArn:
            print('Tagging resource ' + rdsArn)
            rds_client.add_tags_to_resource(ResourceName = rdsArn, Tags=[{'Key': 'Provisioned_By', 'Value': user}, {'Key': 'LOB', 'Value': lob}, {'Key': 'Role', 'Value': rolename}, {'Key': 'Environment', 'Value': env}, {'Key': 'Application', 'Value': application}, {'Key': 'Business_Service', 'Value': business_service}, {'Key': 'Patch_Level', 'Value': patch_level}, {'Key': 'Backup', 'Value': backup}, {'Key': 'Monitoring_Strategy', 'Value': monitoring_strategy}])

        logger.info(' Remaining time (ms): ' + str(context.get_remaining_time_in_millis()) + '\n')
        return True
    except Exception as e:
        logger.error('Something went wrong: ' + str(e))
        traceback.print_exc()
        return False

			
			
			
			

			
