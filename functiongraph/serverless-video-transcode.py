# -*- coding:utf-8 -*-
import os
import json
import shutil
import random
import string
import traceback
import urllib.parse
import subprocess
from obs import ObsClient

LOCAL_MOUNT_PATH = '/tmp/'

taskNum = 5
partSize = 20 * 1024 * 1024
enableCheckpoint = True
code_root = os.environ.get('RUNTIME_CODE_ROOT', '/opt/function/code')


def handler(event, context):
    log = context.getLogger()
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    if ak == "" or sk == "":
        log.error('ak or sk is empty. Please set an agency.')
        raise Exception('ak or sk is empty. Please set an agency.')

    video_transcode_handler = VideoTranscodeHandler(context)
    records = event.get("Records", None)
    try:
        result = video_transcode_handler.run(records[0])
    except Exception as e:
        exec_info = traceback.format_exc()
        log.error(f"video_transcode_handler run error: {exec_info}")
        raise e
    finally:
        video_transcode_handler.clean_local_files(
            video_transcode_handler.download_dir)

    return result


def get_video_info(src_file_path):
    cmd = [code_root + "/ffprobe", "-v", "quiet", "-show_streams",
           "-show_format", "-print_format", "json", "-i", src_file_path]
    video_info_raw = subprocess.check_output(cmd)
    video_info = json.loads(video_info_raw)
    return video_info['streams'][0]["height"], \
           video_info['streams'][0]["width"], \
           video_info['format']["format_name"]


class VideoTranscodeHandler:

    def __init__(self, context):
        self.logger = context.getLogger()
        obs_endpoint = context.getUserData("obs_endpoint")
        self.obs_client = new_obs_client(context, obs_endpoint)
        self.download_dir = gen_local_download_path()
        self.transcode_bucket_name = \
            context.getUserData("transcode_bucket_name")
        self.dst_format = context.getUserData("dst_format")
        self.dst_height = context.getUserData("dst_height")
        self.dst_width = context.getUserData("dst_width")

    def run(self, record):
        (src_bucket, src_object_key) = get_obs_obj_info(record)
        src_object_key = urllib.parse.unquote_plus(src_object_key)
        self.logger.info("src bucket name: %s", src_bucket)
        self.logger.info("src object key: %s", src_object_key)
        self.download_from_obs(src_bucket, src_object_key)
        src_file_path = self.download_dir + src_object_key
        src_height, src_width, src_format = get_video_info(src_file_path)
        if self.dst_format in src_format and \
                self.dst_height == str(src_height) and \
                self.dst_width == str(src_width):
            self.logger.warning("file %s no need to transcode", src_object_key)
            return

        (_, filename) = os.path.split(src_file_path)
        (shortname, ext) = os.path.splitext(filename)
        if self.dst_format == 'm3u8':
            self.m3u8_transcode(shortname, src_file_path)

        else:
            self.transcode(shortname, src_file_path)
        self.logger.info("succeeded to transcode file %s to dts format %s "
                         "width %s height %s", src_object_key, self.dst_format,
                         self.dst_width, self.dst_height)

    def transcode(self, shortname, src_file_path):
        dst_file_path = os.path.join(self.download_dir,
                                     shortname + "." + self.dst_format)
        cmd = [code_root + "/ffmpeg", "-y", "-i", src_file_path,
               "-s", self.dst_width + "x" + self.dst_height, dst_file_path]
        try:
            subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError:
            raise Exception(f"failed to transcode file {src_file_path}, "
                            f"exec_info:{traceback.format_exc()}")

        file_key = os.path.join(self.dst_format, shortname + "." +
                                self.dst_format)
        self.upload_file_to_obs(file_key, dst_file_path)

    def m3u8_transcode(self, shortname, src_file_path):
        m3u8_dir = os.path.join(self.download_dir, "m3u8")
        os.mkdir(m3u8_dir)
        dst_file_path = os.path.join(self.download_dir, shortname + ".ts")
        split_dst_file_path = os.path.join(m3u8_dir, shortname + '_%03d.ts')
        transcode_cmd = [code_root + "/ffmpeg", "-y", "-i", src_file_path,
                         "-s", self.dst_width + "x" + self.dst_height,
                         dst_file_path]
        split_cmd = [code_root + "/ffmpeg", "-y", "-i", dst_file_path, "-c",
                     "copy", "-map", "0", "-f", "segment", "-segment_list",
                     os.path.join(m3u8_dir, shortname + ".m3u8"),
                     "-segment_time", "10", split_dst_file_path]
        try:
            subprocess.run(
                transcode_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                check=True)
            subprocess.run(
                split_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                check=True)
        except subprocess.CalledProcessError:
            raise Exception(f"failed to m3u8 transcode file {src_file_path}, "
                            f"exec_info:{traceback.format_exc()}")

        for filename in os.listdir(m3u8_dir):
            file_path = os.path.join(m3u8_dir, filename)
            file_key = os.path.join("m3u8", shortname, filename)
            self.upload_file_to_obs(file_key, file_path)

    def clean_local_files(self, file_path):
        try:
            shutil.rmtree(file_path, ignore_errors=True)
        except:
            self.logger.error(
                f"failed to clean local file {file_path}, "
                f"exp:{traceback.format_exc()}")

    def download_from_obs(self, bucket, object_key):
        resp = self.obs_client.downloadFile(bucket, object_key,
                                            self.download_dir + object_key,
                                            partSize, taskNum, enableCheckpoint)
        if resp.status >= 300:
            raise Exception(f"failed to download file {object_key} from "
                            f"{bucket}, errorCode:{resp.errorCode} "
                            f"errorMessage:{resp.errorMessage}")

    def upload_file_to_obs(self, object_key, zip_file_name):
        resp = self.obs_client.uploadFile(self.transcode_bucket_name,
                                          object_key, zip_file_name,
                                          partSize, taskNum, enableCheckpoint)
        if resp.status >= 300:
            raise Exception(f"failed to upload file {object_key} to "
                            f"{self.transcode_bucket_name}, "
                            f"errorCode:{resp.errorCode} "
                            f"errorMessage:{resp.errorMessage}")


# generate a temporary directory for downloading things
# from OBS and compress them.
def gen_local_download_path():
    letters = string.ascii_letters
    download_dir = LOCAL_MOUNT_PATH + ''.join(
        random.choice(letters) for i in range(16)) + '/'
    os.makedirs(download_dir)
    return download_dir


def new_obs_client(context, obs_server):
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    return ObsClient(access_key_id=ak, secret_access_key=sk, server=obs_server)


def get_obs_obj_info(record):
    if 's3' in record:
        s3 = record['s3']
        return (s3['bucket']['name'], s3['object']['key'])
    else:
        obs_info = record['obs']
        return (obs_info['bucket']['name'], obs_info['object']['key'])
