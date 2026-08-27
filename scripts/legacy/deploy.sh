#!/bin/bash

# TurboCache 部署脚本
set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 配置
NAMESPACE=${NAMESPACE:-"turbocache"}
DEPLOYMENT_TYPE=${DEPLOYMENT_TYPE:-"kubernetes"}
CONFIG_FILE=${CONFIG_FILE:-"config/config.yaml"}

function info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# 检查 kubectl
function check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        error "kubectl is not installed"
    fi
    info "kubectl version: $(kubectl version --client --short 2>/dev/null || kubectl version --client)"
}

# 检查 docker
function check_docker() {
    if ! command -v docker &> /dev/null; then
        error "docker is not installed"
    fi
    info "docker version: $(docker --version)"
}

# 构建 Docker 镜像
function build_image() {
    local tag="${1:-latest}"
    info "Building Docker image with tag: ${tag}"

    docker build -t turbocache:${tag} -f deployments/docker/Dockerfile .

    info "Docker image built successfully"
}

# 推送镜像
function push_image() {
    local registry="${1}"
    local tag="${2:-latest}"

    if [ -z "$registry" ]; then
        error "Registry not specified"
    fi

    info "Pushing image to ${registry}"

    docker tag turbocache:${tag} ${registry}/turbocache:${tag}
    docker push ${registry}/turbocache:${tag}

    info "Image pushed successfully"
}

# 部署到 Kubernetes
function deploy_k8s() {
    check_kubectl

    info "Deploying to Kubernetes namespace: ${NAMESPACE}"

    # 创建命名空间
    kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

    # 应用配置
    info "Applying Kubernetes manifests..."
    kubectl apply -f deployments/k8s/namespace.yaml
    kubectl apply -f deployments/k8s/configmap.yaml
    kubectl apply -f deployments/k8s/deployment.yaml
    kubectl apply -f deployments/k8s/service.yaml
    kubectl apply -f deployments/k8s/hpa.yaml
    kubectl apply -f deployments/k8s/pdb.yaml

    info "Waiting for deployment to be ready..."
    kubectl rollout status deployment/turbocache -n ${NAMESPACE}

    info "Deployment completed successfully"
}

# 使用 Helm 部署
function deploy_helm() {
    if ! command -v helm &> /dev/null; then
        error "helm is not installed"
    fi

    info "Deploying with Helm to namespace: ${NAMESPACE}"

    helm upgrade --install turbocache deployments/helm \
        --namespace ${NAMESPACE} \
        --create-namespace \
        --values deployments/helm/values.yaml

    info "Helm deployment completed"
}

# Docker Compose 部署
function deploy_compose() {
    check_docker

    info "Deploying with Docker Compose"

    docker-compose -f deployments/docker/docker-compose.yml up -d

    info "Docker Compose deployment completed"
}

# 查看状态
function status() {
    check_kubectl

    info "Checking deployment status in namespace: ${NAMESPACE}"

    echo -e "\n${BLUE}Pods:${NC}"
    kubectl get pods -n ${NAMESPACE}

    echo -e "\n${BLUE}Services:${NC}"
    kubectl get svc -n ${NAMESPACE}

    echo -e "\n${BLUE}Deployments:${NC}"
    kubectl get deployments -n ${NAMESPACE}

    echo -e "\n${BLUE}HPA:${NC}"
    kubectl get hpa -n ${NAMESPACE}
}

# 查看日志
function logs() {
    check_kubectl

    local pod_name="${1}"

    if [ -z "$pod_name" ]; then
        # 获取第一个 pod
        pod_name=$(kubectl get pods -n ${NAMESPACE} -l app=turbocache -o jsonpath='{.items[0].metadata.name}')
    fi

    info "Showing logs for pod: ${pod_name}"
    kubectl logs -f ${pod_name} -n ${NAMESPACE}
}

# 删除部署
function undeploy() {
    check_kubectl

    warn "This will delete all TurboCache resources in namespace: ${NAMESPACE}"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Cancelled"
        return
    fi

    info "Deleting TurboCache deployment..."

    kubectl delete -f deployments/k8s/hpa.yaml || true
    kubectl delete -f deployments/k8s/pdb.yaml || true
    kubectl delete -f deployments/k8s/service.yaml || true
    kubectl delete -f deployments/k8s/deployment.yaml || true
    kubectl delete -f deployments/k8s/configmap.yaml || true

    read -p "Delete namespace ${NAMESPACE}? (y/N) " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete namespace ${NAMESPACE}
    fi

    info "Undeployment completed"
}

# 重启部署
function restart() {
    check_kubectl

    info "Restarting TurboCache deployment in namespace: ${NAMESPACE}"

    kubectl rollout restart deployment/turbocache -n ${NAMESPACE}
    kubectl rollout status deployment/turbocache -n ${NAMESPACE}

    info "Restart completed"
}

# 扩缩容
function scale() {
    check_kubectl

    local replicas="${1}"

    if [ -z "$replicas" ]; then
        error "Number of replicas not specified"
    fi

    info "Scaling TurboCache to ${replicas} replicas"

    kubectl scale deployment/turbocache --replicas=${replicas} -n ${NAMESPACE}

    info "Scaling initiated"
}

# 显示帮助
function show_help() {
    cat << EOF
TurboCache Deployment Script

Usage: $0 [command] [options]

Commands:
    build [tag]           Build Docker image
    push <registry> [tag] Push Docker image to registry
    deploy                Deploy to cluster (default: kubernetes)
    status                Show deployment status
    logs [pod]            Show logs
    restart               Restart deployment
    scale <replicas>      Scale deployment
    undeploy              Remove deployment
    help                  Show this help message

Deployment Types (set via DEPLOYMENT_TYPE):
    kubernetes            Deploy to Kubernetes (default)
    helm                  Deploy with Helm
    compose               Deploy with Docker Compose

Environment Variables:
    NAMESPACE             Kubernetes namespace (default: turbocache)
    DEPLOYMENT_TYPE       Deployment type (default: kubernetes)
    CONFIG_FILE           Config file path (default: config/config.yaml)

Examples:
    $0 build v1.0.0
    $0 push myregistry.com/myproject v1.0.0
    $0 deploy
    DEPLOYMENT_TYPE=helm $0 deploy
    $0 status
    $0 logs
    $0 scale 5
    $0 restart
    $0 undeploy

EOF
}

# 主函数
function main() {
    case "${1}" in
        build)
            check_docker
            build_image "${2:-latest}"
            ;;
        push)
            check_docker
            push_image "${2}" "${3:-latest}"
            ;;
        deploy)
            case "${DEPLOYMENT_TYPE}" in
                kubernetes)
                    deploy_k8s
                    ;;
                helm)
                    deploy_helm
                    ;;
                compose)
                    deploy_compose
                    ;;
                *)
                    error "Unknown deployment type: ${DEPLOYMENT_TYPE}"
                    ;;
            esac
            ;;
        status)
            status
            ;;
        logs)
            logs "${2}"
            ;;
        restart)
            restart
            ;;
        scale)
            scale "${2}"
            ;;
        undeploy)
            undeploy
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            error "Unknown command: $1. Use 'help' for usage information."
            ;;
    esac
}

main "$@"
