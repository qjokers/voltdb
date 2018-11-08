#!/bin/bash -ex

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Functions to deploy VoltDB in Kubernetes, see the associated README


make_statefulset() {
    # customize the k8s statefulset
    MANIFEST=${CLUSTER_NAME}.yaml
    cp voltdb-statefulset.yaml                        $MANIFEST
    SED="sed -i"
    [[ "$OSTYPE" =~ "darwin" ]] && SED="sed -i.bak"
    $SED "s:--clusterName--:$CLUSTER_NAME:g"          $MANIFEST
    $SED "s+--containerImage---+$REP/$IMAGE_TAG+g"    $MANIFEST
    $SED "s:--replicaCount--:$NODECOUNT:g"            $MANIFEST
    $SED "s:--pvolumeSize--:${PVOLUME_SIZE:-1Gi}:g"   $MANIFEST
    $SED "s:--memorySize--:${MEMORY_SIZE:-4Gi}:g"     $MANIFEST
    $SED "s:--cpuCount--:${CPU_COUNT:-1}:g"           $MANIFEST
    rm -f *.bak
}


make_configmap() {

    MAPNAME=${CLUSTER_NAME}-init-configmap
    set +e
    kubectl delete configmap $MAPNAME
    set -e
    CONFIG_MAP_ARGS=""
    [ ! -z "${DEPLOYMENT_FILE}" ] && CONFIG_MAP_ARGS+=" --from-file=deployment=${DEPLOYMENT_FILE}"
    [ ! -z "${CLASSES_JAR}" ] && CONFIG_MAP_ARGS+=" --from-file=classes=${CLASSES_JAR}"
    [ ! -z "${SCHEMA_FILE}" ] && CONFIG_MAP_ARGS+=" --from-file=schema=${SCHEMA_FILE}"
    [ ! -z "${LICENSE_FILE}" ] && CONFIG_MAP_ARGS+=" --from-file=license=${LICENSE_FILE}"

    kubectl create configmap $MAPNAME $CONFIG_MAP_ARGS
}


build_image() {

    # Requires a voltdb kit be installed (and we should be running from it)

    # For voltdb bundles and extensions, manually copy your assets to the corresponding directories
    # prior to creating the image

    if [ ! -d ../../bin ]; then
        echo "ERROR VoltDB tree structure error, VoltDB binaries not found"
        exit -1
    fi

    # Build Tag Deploy the image
    # nb. the docker build environment will encompass the VoltDB kit
    #     the dockerfile removes some content that is not normally required for production

    pushd ../.. > /dev/null

    docker image build -t ${IMAGE_TAG:-$CLUSTER_NAME} \
                       --build-arg VOLTDB_DIST_NAME=$(basename `pwd`) \
                       -f tools/kubernetes/docker/Dockerfile \
                       "$PWD"

    # tag and push as appropriate
    [ -n "${IMAGE_TAG}" ] && docker tag ${IMAGE_TAG} ${REP}/${IMAGE_TAG}
    [ -n "${REP}" ] && docker push ${REP}/${IMAGE_TAG}

    popd > /dev/null

    }


start_voltdb() {
    kubectl scale statefulset ${CLUSTER_NAME} --replicas=$NODECOUNT || kubectl create -f ${CLUSTER_NAME}.yaml
    }


stop_voltdb() {
    # Quiesce the cluster, put it into admin mode, then scale it down
    kubectl exec ${CLUSTER_NAME}-0 -- voltadmin pause --wait
    kubectl scale statefulset ${CLUSTER_NAME} --replicas=0
    }


force_voltdb() {

    # !!! Your voltdb statefulset will be forced to terminate ungracefully

    for P in `kubectl get pods | egrep "^$CLUSTER_NAME" | cut -d\  -f1`
    do
        kubectl delete pods $P --grace-period=0 --force
    done
    kubectl delete statefulset $CLUSTER_NAME --grace-period=0 --force
    kubectl delete service $CLUSTER_NAME
}


purge_persistent_claims() {

    # !!! You will loose your related persistent volumes (all your data), with no possibility of recovery

    for P in `kubectl get pvc | egrep "$CLUSTER_NAME" | cut -d\  -f1`
    do
        kubectl delete pvc $P
    done
}

usage() {
    echo "Usage: $0 parameter-file options ..."
    echo "      -B --build-voltdb-image         Builds a docker image of voltdb and pushes it to your repo"
    echo "      -M --install-configmap          Run kubectl create configmap for your config"
    echo "      -C --configure-voltdb           Customize the voltdb-manifest with provided parameters"
    echo "      -S --start-voltdb               Run kubectl create <voltdb-manifest>"
    echo "      -D --purge-persistent-volumes   Run kubectl delete on clusters persistent volumes"
    echo "      -F --force-voltdb-down          Run kubectl delete cluster statefulset and running pods"
    exit 1
}


# MAIN

# source the template settings
if [ ! -e "$1" ]; then
    echo "ERROR parameter file not specified, customize the template with your settings and database assets"
    usage
fi

# parse out the parameter file filename parts
#_EXT=$([[ "$1 = *.* ]] && echo ".${1##*.}" || echo '')
CLUSTER_NAME="${1%.*}"

# source our config file parameters
source $1

shift 1

# use Cluster name as default image name
: ${IMAGE_TAG:=${CLUSTER_NAME}}

if [ $# -eq 0 ]; then
    echo "ERROR option(s) missing"
    usage
fi

for cmd in "$@"
do
    case $cmd in

        -B|--build-voltdb-image)
                                    build_image
        ;;
        -M|--install-configmap)
                                    make_configmap
        ;;
        -C|--configure-voltdb)
                                    make_statefulset
        ;;
        -S|--start-voltdb)
                                    start_voltdb
        ;;
        -P|--stop-voltdb)
                                    stop_voltdb
        ;;
        -D|--purge-persistent-claims)
                                    purge_persistent_claims
        ;;
        -F|--force-voltdb-set)
                                    force_voltdb
        ;;
        -R|--restart_voltdb)
                                    stop_voltdb
                                    start_voltdb
        ;;
        -h|--help)
                                    usage
        ;;
        *)
                                    echo "ERROR unrecognized option"
                                    usage
                                    exit 1
        ;;
    esac
done
