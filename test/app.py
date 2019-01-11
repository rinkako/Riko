# from src import riko


class A:
    @staticmethod
    def foo():
        print("wow")


class B(A):
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
    print("Hello Riko!")
