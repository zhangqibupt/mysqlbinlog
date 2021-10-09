@Library('jenkins-shared-library@master') _
import freewheel.ep.share.*
ep_tools = new ep_public_lib()

def Triggered_Build

def getServiceName(GIT_URL){
  service_name = GIT_URL.split('/')[4].split('.git')[0]
  println "service_name: ${service_name}"
  return service_name
}

pipeline{
  agent {
    label 'ui-slave-base'
  }
  options {
    timeout(time: 2, unit: 'HOURS')
    timestamps()
    ansiColor('xterm')
  }
  environment {
    githubCredId = "jenkins-github"
    service_name = getServiceName(GIT_URL)
  }

  stages {
    stage('Get Release Parameters') {
      steps{
        script{
          //EP will auto inject these parameters to environment by call: auto_inject_release_env_parameters
          ep_tools.auto_inject_release_env_parameters()
          sh '''
           env
          '''
        }
      }
    }

    stage("Bricks_CI_Trigger") {
      steps{
        script {
          if ("${BRANCH_NAME}" =~ "^PR-") {
            // subci trigger
            Triggered_Build = build(job: "UI/UI_CI/go_modules/bricks/Bricks_SubCI_Pipeline", propagate: false, parameters: [
            string(name: "GITHUB_TRIGGER", value: "true"), string(name: "GITHUB_PR_COMMIT", value: "${GIT_COMMIT}"), string(name: "service_name", value: "${service_name}")
            ])
          }
          if(env.TAG_NAME && env.TAG_NAME != "") {
            // released tag trigger
            Triggered_Build = build(job: "UI/UI_CI/go_modules/bricks/Bricks_Package_Release_Pipeline", propagate: false, parameters: [
            string(name: "GITHUB_TRIGGER", value: "true"), string(name: "service_name", value: "${service_name}"), string(name: "publishVersion", value: "${TAG_NAME}")
            ])
          }
          if (Triggered_Build) {
              Downstream_Url = Triggered_Build.absoluteUrl
              Downstream_Res = Triggered_Build.result
              echo "DownStream Build Result: ${Downstream_Res} \n${Downstream_Url}"
              if (Downstream_Res != "SUCCESS") {
                  err_message = "Build Result: ${Downstream_Res}"
                  ep_tools.highlight_info("error", err_message)
              }
          }
        }
      }
    }
  }

  post {
    cleanup {
      cleanWs()
    }
  }

}

