# 1. First, remove already installed mysql-server using--
sudo apt-get remove --purge mysql-server mysql-client mysql-common
# 2. Then clean all files
sudo apt-get autoremove
sudo apt-get remove -y mysql-*
sudo apt-get purge -y mysql-*
# 3. Update Packages
sudo apt update
