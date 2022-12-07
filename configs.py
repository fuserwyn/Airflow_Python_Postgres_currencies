import os
from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()
print(fernet_key.decode())  # your fernet_key, keep it in secured place!
# with open('token.env', 'r') as file:
#     line = file.readline()
#     try:
#         os.environ[line[:line.find("=")]] = line[line.find("=")+1:]
#     except ValueError:
#         pass
# API_KEY = os.environ['API_KEY']
