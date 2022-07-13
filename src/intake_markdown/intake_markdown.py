from intake.source.base import DataSource, Schema
from markdown import markdown

with open('styles.css', 'r') as f:
    default_styles = f.read()


class Markdown:
    def __init__(self, data, extensions=['fenced_code', 'codehilite', 'md_in_html'],
                 extension_configs={},
                 pre="", post="", styles=default_styles):
        self.data = data
        self.extensions = extensions
        self.extension_configs = extension_configs
        self.pre = pre
        self.post = post
        self.styles = styles

    def get_markdown(self):
        return self.pre + self.data + self.post

    def _repr_html_(self):
        style_html = f"<style>\n{self.styles}\n</style>\n"
        md = markdown(self.get_markdown(), extensions=self.extensions,
                        extension_configs=self.extension_configs)
        return style_html + md


class MarkdownSource(DataSource):
    """Reads markdown files and writes them to Markdown objects.
    Based on Intake's `textfile` driver 
    Takes a set of files, and returns a single Markdown object
    The `Markdown object is built by concatenating each file's contents.
    The files can be local or remote. Extra parameters for encoding, etc.,
    go into ``storage_options``.
    """
    name = 'markdown'
    version = '0.0.1'
    container = 'markdown'
    partition_access = True

    def __init__(self, urlpath, text_encoding='utf8',
                 compression=None, metadata=None,
                 md_kwargs={}, storage_options=None):
        """
        Parameters
        ----------
        urlpath : str or list(str)
            Target files. Can be a glob-path (with "*") and include protocol
            specified (e.g., "s3://"). Can also be a list of absolute paths.
        text_encoding : str
            If text_mode is True, apply this encoding. UTF* is by far the most
            common
        compression : str or None
            If given, decompress the file with the given codec on load. Can
            be something like "gzip", "bz2", or to try to guess from the filename,
            'infer'
        storage_options: dict
            Options to pass to the file reader backend, including text-specific
            encoding arguments, and parameters specific to the remote
            file-system driver, if using.
        pre: str 
            If given, appends contents before read markdown str before passing to
            IPython Mardown object constructor
        post: str 
            If given, appends contents after read markdown str before passing to
            IPython Mardown object constructor
        """
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._dataframe = None
        self._files = None
        self.compression = compression
        self.mode = 'rt'
        self.encoding = text_encoding
        self.md_kwargs = md_kwargs

        super(MarkdownSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from fsspec import open_files
        if self._files is None:

            urlpath = self._get_cache(self._urlpath)[0]

            self._files = open_files(
                urlpath, mode=self.mode, encoding=self.encoding,
                compression=self.compression,
                **self._storage_options)
            self.npartitions = len(self._files)
        return Schema(dtype=None,
                      shape=(None, ),
                      npartitions=self.npartitions,
                      extra_metadata=self.metadata)

    def _get_partition(self, i):
        return Markdown(get_file(self._files[i]))

    def read(self):
        self._get_schema()
        file_data = [get_file(f) for f in self._files]
        string = ''.join(file_data)
        md = Markdown(string, **self.md_kwargs)
        return md


def get_file(f):
    """Serializable function to take an OpenFile object and read lines"""
    with f as f:
        d = f.read()
    return d
