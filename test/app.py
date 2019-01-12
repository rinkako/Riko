# from src import riko
from abc import ABCMeta, abstractmethod


class A:
    __metaclass__ = ABCMeta

    pk = []

    @staticmethod
    def foo():
        print("wow")

    @abstractmethod
    def get_pk(self):
        return self.pk


class B(A):
    pk = ['id', 'gg']

    @staticmethod
    def foo():
        print("foo1 replaced")

    @staticmethod
    def foo2():
        print("wow2")


if __name__ == '__main__':
    b = B()
    b.foo()
    b.foo2()
    t = b.get_pk()
    print("Hello Riko!")
