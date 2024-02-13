import gcsfs


class GCPPlatform:
    """Google Cloud Platform specific methods"""

    def __init__(self):
        self.fs = gcsfs.GCSFileSystem()
