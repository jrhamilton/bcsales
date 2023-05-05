#!/bin/bash


echo "Installing docker-compose..."
cd 
mkdir -p bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.17.1/docker-compose-linux-x86_64 -O docker-compose
sudo chmod +x docker-compose

echo "Setup .bashrc..."
echo '' >> ~/.bashrc
echo 'export PATH=${HOME}/bin:${PATH}' >> ~/.bashrc
export PATH=${HOME}/bin:${PATH}

echo "Execute: source ~/.bashrc"
echo "Execute: docker-compose version"

cd $HOME
