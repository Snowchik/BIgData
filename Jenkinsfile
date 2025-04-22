pipeline {
    agent any

    environment {
        DB_HOST = 'localhost'
        DB_PORT = '5432'
        DB_USER = 'forum_user'
        DB_PASSWORD = 'forum_pass'
        DB_NAME = 'forum_logs'
    }

    stages {
        stage('Clone repo') {
            steps {
                git 'https://your-repo-url.git'
            }
        }

        stage('Install dependencies') {
            steps {
                sh 'pip install -r requirements.txt'
            }
        }

        stage('Run ETL') {
            steps {
                sh 'python aggregate_logs.py 2025-04-01 2025-04-30'
            }
        }
    }

    triggers {
        cron('H H * * *')
    }
}
