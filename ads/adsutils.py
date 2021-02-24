
def HexBlock(data, width=8):
    i = 0
    count = 0
    result = ''
    currentHexLine = ''
    currentChrLine = ''

    for byte in data:
        # next line, if required
        if (i == width):
            result += f'{currentHexLine} {currentChrLine}\n'
            currentHexLine = ''
            currentChrLine = ''
            i = 0

        if i==0 :
            result += f'{count:04X}: '
        char = ord(byte) if isinstance(byte, str) else byte


        # append to lines
        if (i>0) and (i%4==0):
            currentHexLine += '| '
        currentHexLine += f'{char:02x} '
        currentChrLine += '.' if (char < 32 or char > 126) else chr(char)
        i += 1
        count += 1
    # append last line
    if len(currentHexLine) < width * 3 + (width // 4 - 1) * 2 + 7:
        currentHexLine += ' ' * (width * 3 + (width // 4 - 1) * 2 - len(currentHexLine))
    result += f'{currentHexLine} {currentChrLine}'
    return result
