# Build environment
conda deactivate
conda conda remove --name deepcvr --all
conda create --name deepcvr python=3.8
conda activate deepcvr
# Install packages from conda
conda install -c conda-forge jupyter-book
conda install boto3
conda install tqdm
conda install -c conda-forge progressbar
conda install ipykernel
# Install Mysql
sudo dpkg -i setup/MySQL/mysql-apt-config_0.8.22-1_all.deb
# pip install
pip install ghp-import