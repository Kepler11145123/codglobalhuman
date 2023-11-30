#!/usr/bin/env groovy

@Library("osiris-docker-module@master") l1
@Library("osiris-core-module@master") l2
@Library("osiris-samuel-module") l3
@Library("sonar@lts") l4
@Library("osiris-conf-module@master") l5


node("spark") {
    try {
        stage('Checkout') {
            checkout scm
        }
        dev("pipeline")
    } finally {
        cleanWs()
    }
}