
import time

def fibo_gen(max):
    n1 = 0 
    n2 = 1
    counter = 0 
    while True:
        if n2 >= max:
            raise Exception(f"Se supero el maiximo definido ({max})")
            raise StopIteration

        elif counter == 0:
            counter += 1
            yield n1
        elif counter == 1:
            counter += 1
            yield n2
        else:
            aux = n1 + n2
            n1, n2 = n2, aux
            counter +=1
            yield aux

if __name__ == '__main__':
    fibonacci = fibo_gen(20)
    for e in fibonacci:
        print(e)