from intake_bluesky.jsonl import BlueskyJSONLCatalog
from xicam.plugins import CatalogPlugin
from qtpy.QtWidgets import QFileDialog
import os
import glob


class JSONLCatalogPlugin(BlueskyJSONLCatalog, CatalogPlugin):
    name = "JSONL"

    def __init__(self):
        # For proof of concept, this plugin will ask the user for a directory to load from
        dir = QFileDialog.getExistingDirectory(caption='Load JSONL from directory',
                                               directory=os.path.expanduser('~/'))

        # TODO: Move directory dialog to the controller

        paths = glob.glob(os.path.join(dir, '*.jsonl'))
        super(JSONLCatalogPlugin, self).__init__(paths)

        self.name = f"JSONL: {dir}"
