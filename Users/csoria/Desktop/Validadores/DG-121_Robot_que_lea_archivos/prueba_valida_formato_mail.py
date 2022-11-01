def validar(mail):
    if mail.count("@")!=1: return False 
    name,dom=mail.split('@')
    if len(name)>=5 and ("." in dom):
        dom = f"@{dom}"
        if dom.index(".") in (len(dom), 1): return False
        return True 
    else: return False 

print(validar('holacomoestas@validar.com'))
print(validar('holacomoestas@.com'))
