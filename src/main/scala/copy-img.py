import shutil
import os
from time import sleep
import time
source_dir = "src/main/resources/L0001"
target_dir = "src/main/resources/images"

file_names = os.listdir(source_dir)

for file_name in file_names:
    shutil.copy(os.path.join(source_dir, file_name), target_dir)
