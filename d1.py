import os
import paramiko
import threading
import time


def sftp_transfer_thread(event, local_folder, remote_path, url, port, username, password):
    transport = paramiko.Transport((url, port))
    transport.connect(username=username, password=password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.chdir(remote_path)
    files = sftp.listdir()

    for file in files:
        remote_file = remote_path + '/' + file
        local_file = os.path.join(local_folder, file)

        if not os.path.exists(local_file):
            sftp.get(remote_file, local_file)
            print(f"Downloaded: {remote_file} -> {local_file}")

    sftp.close()
    transport.close()

    event.set()

def sftp_download(url, port, username, password, sftp_path, folder_path):
    transport = paramiko.Transport((url, port))
    transport.connect(username=username, password=password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.chdir(sftp_path)
    files = sftp.listdir()
    os.makedirs(folder_path, exist_ok=True)

    thread_count = 10
    events = [threading.Event() for _ in range(thread_count)]
    threads = []

    for i in range(thread_count):
        t = threading.Thread(target=sftp_transfer_thread, args=(events[i], folder_path, sftp_path, url, port, username, password))
        threads.append(t)
        t.start()
        time.sleep(3)

    for event in events:
        event.wait()

    sftp.close()
    transport.close()


sftp_download("direct.virtaicloud.com", 19001, "xrVEMpYjvp", "lBlAjcCIxO", "/upload/ComfyUI/custom_nodes/ComfyUI-WD14-Tagger/models", "/ComfyUI/custom_nodes/ComfyUI-WD14-Tagger/models")
sftp_download("direct.virtaicloud.com", 19001, "xrVEMpYjvp", "lBlAjcCIxO", "/upload/ComfyUI/custom_nodes/comfyui_controlnet_aux/ckpts/models--yzd-v--DWPose/snapshots/1a7144101628d69ee7a3768d1ee3a094070dc388", "/ComfyUI/custom_nodes/comfyui_controlnet_aux/ckpts/models--yzd-v--DWPose/snapshots/1a7144101628d69ee7a3768d1ee3a094070dc388")
sftp_download("direct.virtaicloud.com", 19001, "xrVEMpYjvp", "lBlAjcCIxO", "/upload/ComfyUI/custom_nodes/comfyui_controlnet_aux/ckpts/models--yzd-v--DWPose/refs", "/ComfyUI/custom_nodes/comfyui_controlnet_aux/ckpts/models--yzd-v--DWPose/refs")
