- Install docker: sudo apt-get install docker.io
-   sudo usermod -aG docker ${USER}
    su - ${USER}
    su - mine44
    sudo groupadd docker
    sudo usermod -a -G docker  $USER
    sudo apt-get install python3-pip
- Install docker-compose: curl -SL https://github.com/docker/compose/releases/download/v2.14.2/docker-compose-linux-x86_64 -o /usr/bin/docker-compose
- Executable permissions to the standalone binary: sudo chmod +x /usr/bin/docker-compose
