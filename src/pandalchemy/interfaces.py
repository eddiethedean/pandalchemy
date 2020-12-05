import abc


class IDataBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, engine):
        raise NotImplementedError

    @abc.abstractmethod
    def __getitem__(self, key):
        raise NotImplementedError

    @abc.abstractmethod
    def __setitem__(self, key, value):
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def table_names(self):
        raise NotImplementedError

    @abc.abstractmethod
    def pull(self):
        raise NotImplementedError


class ITable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, name, data, key, f_keys=[], types=dict()):
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __setitem__(self, key, value):
        raise NotImplementedError

    @abc.abstractproperty
    def column_names(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __getitem__(self, key):
        raise NotImplementedError

    @abc.abstractmethod
    def drop(self, *args, **kwargs):
        raise NotImplementedError
