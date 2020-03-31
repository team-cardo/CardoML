from typing import Tuple, Dict, Any, Iterable, Callable


class RegisterChildren(type):
    """
    when used as a metaclass register classes that inherit from the class
    Example:
        class A(metaclass=RegisterChildren): pass
        class B(A): pass
        class C(A): pass

        for cls in A:
              print(cls.__name__)
        output:
        C
        B
    """

    def __init__(cls, name: str, bases: Tuple[type], nmspc: Dict[str, Any]):
        super().__init__(name, bases, nmspc)
        if not hasattr(cls, 'registry'):
            cls.registry = set()
        cls.registry.add(cls)
        cls.registry -= set(bases)

    def __iter__(cls) -> Iterable[Callable]:
        return iter(cls.registry)
