from configparser import SafeConfigParser
from collections.abc import Mapping


class ConfigParser(Mapping):
    """ConfigParser class"""

    def __init__(self, config_loc: str, section: str = 'aws'):
        """Constructor"""
        self.config_loc = config_loc
        self.section = section
        self.config = SafeConfigParser()
        self.config.read(self.config_loc)

        self._config_dict = dict(self.config.items(self.section))

    def __getitem__(self, key):
        """Get item"""
        return self._config_dict[key]

    def __iter__(self):
        """Iterate"""
        return iter(self._config_dict)

    def __len__(self):
        """Length"""
        return len(self._config_dict)
