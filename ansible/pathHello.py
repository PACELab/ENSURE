import commands,socket,time,math
def main(args):
    name = args.get("name", "stranger")
    commands.getoutput("sleep 5")
    hostname = socket.gethostname()
    greeting = "Hello " + name + " ! socket --> "+ hostname
    print(greeting)
    return {"greeting": greeting}
