import time
import os
import random
from sys import platform

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if platform == "win32" else 'clear')

def style_1():
    """Block style ASCII art"""
    return r"""
██████╗ ██╗   ██╗███████╗ █████╗ ███╗   ███╗███████╗██████╗ 
██╔══██╗██║   ██║██╔════╝██╔══██╗████╗ ████║██╔════╝██╔══██╗
██████╔╝██║   ██║███████╗███████║██╔████╔██║█████╗  ██████╔╝
██╔══██╗██║   ██║╚════██║██╔══██║██║╚██╔╝██║██╔══╝  ██╔══██╗
██║  ██║╚██████╔╝███████║██║  ██║██║ ╚═╝ ██║███████╗██║  ██║
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚═╝  ╚═╝
    """

def style_2():
    """Outline style ASCII art"""
    return r"""
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   ██▀███  █    ██  ▄▄▄       ███▄    █  ███▄ ▄███▓     │
│  ▓██ ▒ ██▒ ██  ▓██▒▒████▄     ██ ▀█   █ ▓██▒▀█▀ ██▒     │
│  ▓██ ░▄█ ▒▓██  ▒██░▒██  ▀█▄  ▓██  ▀█ ██▒▓██    ▓██░     │
│  ▒██▀▀█▄  ▓▓█  ░██░░██▄▄▄▄██ ▓██▒  ▐▌██▒▒██    ▒██      │
│  ░██▓ ▒██▒▒▒█████▓  ▓█   ▓██▒▒██░   ▓██░▒██▒   ░██▒     │
│  ░ ▒▓ ░▒▓░░▒▓▒ ▒ ▒  ▒▒   ▓▒█░░ ▒░   ▒ ▒ ░ ▒░   ░  ░     │
│    ░▒ ░ ▒░░░▒░ ░ ░   ▒   ▒▒ ░░ ░░   ░ ▒░░  ░      ░     │
│    ░░   ░  ░░░ ░ ░   ░   ▒      ░   ░ ░ ░      ░        │
│     ░        ░           ░  ░         ░        ░        │
│                                                         │
└─────────────────────────────────────────────────────────┘
    """

def style_3():
    """Banner style ASCII art"""
    return r"""
.-.-. .-.-. .-.-. .-.-. .-.-. .-.-. .-.-.
|R|U| |S|A| |M|E| |R| | |B|Y| | |Y|O|U|
`-'`-' `-'`-' `-'`-' `-'`-' `-'`-' `-'-'
    """

def style_4():
    """3D style ASCII art"""
    return r"""
  ____  _   _ ____    _    __  __ ______ 
 |  _ \| | | / ___|  / \  |  \/  |  _ \ \
 | |_) | | | \___ \ / _ \ | |\/| | | | | |
 |  _ <| |_| |___) / ___ \| |  | | |_| | |
 |_| \_\\___/|____/_/   \_\_|  |_|____/| |
                                     \_\_\
    """

def style_5():
    """Graffiti style ASCII art"""
    return r"""
╔═╗┬─┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
║╣ ├┬┘├┤  │ │ ││││ │ 
╚═╝┴└─└─┘ ┴ └─┘┘└┘ ┴ 
    """

def animate_text(text, delay=0.1):
    """Animate text appearing one character at a time"""
    for char in text:
        print(char, end='', flush=True)
        time.sleep(delay)
    print()

def color_text(text, color_code):
    """Add color to text using ANSI escape codes"""
    return f"\033[{color_code}m{text}\033[0m"

def main():
    styles = [style_1, style_2, style_3, style_4, style_5]
    colors = [
        '91',  # Red
        '92',  # Green
        '93',  # Yellow
        '94',  # Blue
        '95',  # Magenta
        '96',  # Cyan
    ]
    
    try:
        while True:
            clear_screen()
            style = random.choice(styles)
            color = random.choice(colors)
            
            ascii_art = style()
            colored_art = color_text(ascii_art, color)
            
            print(colored_art)
            
            # Add some decorative elements
            border = color_text("✧" * 40, random.choice(colors))
            print(border)
            
            message = color_text("Welcome to RUsamer Script!", random.choice(colors))
            animate_text(message, 0.05)
            
            print(border)
            
            time.sleep(3)
            
    except KeyboardInterrupt:
        clear_screen()
        print("Thanks for using RUsamer!")
        exit_art = color_text(style_3(), '92')
        print(exit_art)

if __name__ == "__main__":
    main()
