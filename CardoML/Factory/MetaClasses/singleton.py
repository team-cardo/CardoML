INSTANCE = "__instance"


class Singleton(type):
    """
    When used as a metaclass, force a class to have only 1 instance
    creating the class again will return the existing instance
    """
    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, INSTANCE):
            setattr(cls, INSTANCE, super().__call__(*args, **kwargs))
        return getattr(cls, INSTANCE)
