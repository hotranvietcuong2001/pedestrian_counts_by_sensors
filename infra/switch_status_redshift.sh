#!/bin/bash

REDSHIFT_CLUSTER_STATUS=$(aws redshift describe-clusters --cluster-identifier vc-pedestrian-sensor-redshift-cluster \
                            --query 'Clusters[0].ClusterStatus' --output text)

if [[ $REDSHIFT_CLUSTER_STATUS == "" ]]; then
    aws redshift restore-from-cluster-snapshot --cluster-identifier vc-pedestrian-sensor-redshift-cluster \
        --snapshot-identifier latest-vc-pedestrian-sensor-redshift-cluster
    
    
    
    while :
    do
        echo "Waiting for Redshift cluster to start, sleeping for 60s before next check"
        sleep 60
        NEXT_REDSHIFT_CLUSTER_STATUS=$(aws redshift describe-clusters --cluster-identifier vc-pedestrian-sensor-redshift-cluster \
                                        --query 'Clusters[0].ClusterStatus' --output text)
        if [[ "$NEXT_REDSHIFT_CLUSTER_STATUS" == "available" ]]; then
            break
        fi
    done
    echo "Done. Removing Snapshot"
    aws redshift delete-cluster-snapshot --snapshot-identifier latest-vc-pedestrian-sensor-redshift-cluster

elif [[ $REDSHIFT_CLUSTER_STATUS == "available" ]]; then
    aws redshift delete-cluster --cluster-identifier vc-pedestrian-sensor-redshift-cluster \
        --final-cluster-snapshot-identifier latest-vc-pedestrian-sensor-redshift-cluster
else
    echo "Waiting..."
    echo "Try later"
fi
