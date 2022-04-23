from ipaddress import ip_address
import os

ip_address=["10.3.5.211","10.3.5.208","10.3.5.204","10.3.5.205"]

for ip in ip_address:
    file_path="C:\\Users\\Admin\\Documents\\4thSem\\DDS\\Phase-2\\2PC.py"
    os.system("scp {} user@{}:/home/user/xmen/".format(file_path,ip))