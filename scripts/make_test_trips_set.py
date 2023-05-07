
SALTO_DE_LINEA = 20
NUMERO_DE_TRIPS = 1000000
PATH = "../.data/washington/trips.csv"

def main():
    file = open("trips.csv", "w")
    f = open(PATH, 'r')
    row_header = f.readline()
    file.write(row_header)
    lines = 0
    eof = False
    while lines < NUMERO_DE_TRIPS:
        for _i in range(SALTO_DE_LINEA):
            line = f.readline()
            if not line: 
                eof = True
                break
        if eof: break
        file.write(line)
        lines += 1
    file.close()
    f.close()

main()