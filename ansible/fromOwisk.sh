#! /bin/bash
ansible-playbook -i environments/distributed openwhisk.yml

ansible-playbook -i environments/distributed postdeploy.yml
ansible-playbook -i environments/distributed apigateway.yml
ansible-playbook -i environments/distributed routemgmt.yml
wsk property set --auth $(cat files/auth.guest) --apihost serverless-8
