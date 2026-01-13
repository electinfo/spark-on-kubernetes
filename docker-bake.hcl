// docker-bake.hcl - Build all Spark/Zeppelin images
// Usage: docker buildx bake [target]
//
// Build order: spark-base first, then all others (they depend on it)

variable "REGISTRY" {
  default = "registry.container-registry.svc.cluster.local:5000"
}

variable "TAG" {
  default = "latest"
}

// Default builds everything
group "default" {
  targets = ["spark-base", "spark-images", "zeppelin-images"]
}

// Just Spark images (executor, driver, connect server, runner)
group "spark-images" {
  targets = ["spark-executor", "spark-driver", "spark-connect-server", "spark-runner"]
}

// Just Zeppelin images
group "zeppelin-images" {
  targets = ["zeppelin-server", "zeppelin-interpreter"]
}

// Base image - must be built first
target "spark-base" {
  context = "images/spark-base"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/spark-base:${TAG}"]
  platforms = ["linux/amd64"]
}

// Spark images - depend on spark-base
target "spark-executor" {
  context = "images/spark-executor"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/spark-executor:${TAG}"]
  platforms = ["linux/amd64"]
  args = {
    REGISTRY = "${REGISTRY}"
  }
  contexts = {
    "${REGISTRY}/electinfo/spark-base:latest" = "target:spark-base"
  }
}

target "spark-driver" {
  context = "images/spark-driver"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/spark-driver:${TAG}"]
  platforms = ["linux/amd64"]
  args = {
    REGISTRY = "${REGISTRY}"
  }
  contexts = {
    "${REGISTRY}/electinfo/spark-base:latest" = "target:spark-base"
  }
}

target "spark-connect-server" {
  context = "images/spark-connect-server"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/spark-connect-server:${TAG}"]
  platforms = ["linux/amd64"]
  args = {
    REGISTRY = "${REGISTRY}"
  }
  contexts = {
    "${REGISTRY}/electinfo/spark-base:latest" = "target:spark-base"
  }
}

// Spark runner - depends on spark-connect-server, adds sbt for Scala builds
target "spark-runner" {
  context = "images/spark-runner"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/spark-runner:${TAG}"]
  platforms = ["linux/amd64"]
  args = {
    REGISTRY = "${REGISTRY}"
  }
  contexts = {
    "${REGISTRY}/electinfo/spark-connect-server:latest" = "target:spark-connect-server"
  }
}

// Zeppelin images - depend on spark-base
target "zeppelin-server" {
  context = "images/zeppelin-server"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/zeppelin-server:${TAG}"]
  platforms = ["linux/amd64"]
  args = {
    REGISTRY = "${REGISTRY}"
  }
  contexts = {
    "${REGISTRY}/electinfo/spark-base:latest" = "target:spark-base"
  }
}

target "zeppelin-interpreter" {
  context = "images/zeppelin-interpreter"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/electinfo/zeppelin-interpreter:${TAG}"]
  platforms = ["linux/amd64"]
  args = {
    REGISTRY = "${REGISTRY}"
  }
  contexts = {
    "${REGISTRY}/electinfo/spark-base:latest" = "target:spark-base"
  }
}