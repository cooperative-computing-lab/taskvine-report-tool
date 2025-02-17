class PeerTransfer:
    def __init__(self, filename, manager_info):
        self.manager_info = manager_info

        self.filename = filename
        self.size_mb = None

        self.source = []
        self.destination = []
        self.cache_level = []

        self.when_start_stage_in = []
        self.when_stage_in = []
        self.when_stage_out = []


    def set_size_mb(self, size_mb):
        if self.size_mb and size_mb != self.size_mb:
            raise ValueError(f"size mismatch for {self.filename}")
        self.size_mb = size_mb

    def set_when_start_stage_in(self, when_start_stage_in):
        self.when_start_stage_in.append(when_start_stage_in)

    def set_cache_level(self, cache_level):
        cache_level = int(cache_level)
        if self.cache_level and cache_level != self.cache_level[-1]:
            raise ValueError(f"cache level mismatch for {self.filename}")
        self.cache_level.append(cache_level)

    def append_when_start_stage_in(self, when_start_stage_in):
        self.when_start_stage_in.append(when_start_stage_in)

    def append_when_stage_in(self, when_stage_in):
        self.when_stage_in.append(when_stage_in)

    def append_when_stage_out(self, when_stage_out):
        self.when_stage_out.append(when_stage_out)

    def append_source(self, source):
        self.source.append(source)

    def append_destination(self, destination):
        self.destination.append(destination)
