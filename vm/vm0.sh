#!/bin/bash

echo "UPDATE and UPGRADE the system"
sudo apt-get -y update && sudo apt-get -y upgrade

echo "INSTALL essential programs"
sudo apt-get -y install build-essential python3-pip vim neovim tmux wget unzip

echo "INSTALL pip requirements"
pip install -r $HOME/bcsales/vm/requirements.txt

echo "Aliases"
echo 'alias v=nvim' >> $HOME/.bashrc
echo 'alias p=python3' >> $HOME/.bashrc
alias v=nvim
alias p=python3

echo "CREATING .creds/gcp dir for GOOGLE_APPLICATION_CREDENTIAL file"
CRED_DIR=$HOME/.creds/gcp
[ ! -d "$CRED_DIR" ] && mkdir -p $CRED_DIR

echo "---"
echo "---"
echo "*** GENERATING ssh-keygen.***"
echo "*** ACCEPT ALL DEFAULTS ***"
SSH_FILE=$HOME/.ssh/ed25519.pub
[ ! -f  "$SSH_FILE" ] && ssh-keygen -t ed25519

curl -sSL https://raw.githubusercontent.com/alacritty/alacritty/master/extra/alacritty.info | tic -x -

echo "set -g default-terminal \"screen-256color\"
set-option -sg escape-time 10
set-option -g focus-events on
set-option -sa terminal-overrides ',tmux-256color:RGB'" > $HOME/.tmux.conf

echo "**********************************************"
echo "**********************************************"
echo "\n\n"
echo "Before continuing, follow the next 2 steps:"
echo ".."
echo "1) Copy ~/.ssh/id_ed25519.pub to GOOGLE CLOUD PLATFORM metadata ssh"
echo -n "Hit Enter when the First step above is complete."
echo ".."
echo "2) Copy your GOOGLE_APPLICATION_CREDENTIAL file from local cpu to this machine:"
echo ".."
echo "   Open another terminal on your local computer and scp your google application credentials to the remote instance."
echo ".."
echo "   EXAMPLE: scp ~/.google/credentials/my-gcp-cred-XXXX.json me@VM_IP_ADDRESS:~/.creds/gac/gac.json"
echo ".."
echo "   This will allow this machine to ssh into your initialized machines."
echo ".."
echo -n "Hit Enter when the Second step is complete."
