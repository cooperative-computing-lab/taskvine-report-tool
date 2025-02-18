class PeerTransfer:
    def __init__(self, filename):
        self.filename = filename
        self.size_mb = None

        self.source = []
        self.destination = []
        self.cache_level = []
        self.type = []

        self.when_start_stage_in = []
        self.when_stage_in = []
        self.when_stage_out = []

    def set_size_mb(self, size_mb):
        if self.size_mb and size_mb != self.size_mb:
            raise ValueError(f"size mismatch for {self.filename}")
        self.size_mb = size_mb

    def append_cache_level(self, cache_level):
        """
        /** Control caching and sharing behavior of file objects. **/
        typedef enum {
            VINE_CACHE_LEVEL_TASK = 0,     /**< Do not cache file at worker. (default) */
            VINE_CACHE_LEVEL_WORKFLOW = 1, /**< File remains in cache of worker until workflow ends. */
            VINE_CACHE_LEVEL_WORKER = 2,   /**< File remains in cache of worker until worker terminates. */
            VINE_CACHE_LEVEL_FOREVER = 3   /**< File remains at execution site when worker terminates. (use with caution) */
        } vine_cache_level_t;
        """
        cache_level = int(cache_level)
        if self.cache_level and cache_level != self.cache_level[-1]:
            raise ValueError(f"cache level mismatch for {self.filename}")
        self.cache_level.append(cache_level)
    
    def append_type(self, type):
        """
        /** The type of an input or output file to attach to a task. */
        typedef enum {
            VINE_FILE = 1,              /**< A file or directory present at the manager. **/
            VINE_URL,                   /**< A file obtained by downloading from a URL. */
            VINE_TEMP,                  /**< A temporary file created as an output of a task. */
            VINE_BUFFER,                /**< A file obtained from data in the manager's memory space. */
            VINE_MINI_TASK,             /**< A file obtained by executing a Unix command line. */
        } vine_file_type_t;
        """
        type = int(type)
        if self.type and type != self.type[-1]:
            raise ValueError(f"type mismatch for {self.filename}")
        self.type.append(type)

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

    def print_info(self):
        print(f"filename: {self.filename}")
        print(f"size_mb: {self.size_mb}")
        print(f"source: {self.source}")
        print(f"destination: {self.destination}")
        print(f"cache_level: {self.cache_level}")
        print(f"type: {self.type}")
        print(f"when_start_stage_in: {self.when_start_stage_in}")
        print(f"when_stage_in: {self.when_stage_in}")
        print(f"when_stage_out: {self.when_stage_out}")
        print(f"\n")
