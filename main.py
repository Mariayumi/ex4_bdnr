from cmath import log
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


print("comecou.........")
cloud_config= {'secure_connect_bundle': 'secure-connect-cassandra.zip'}

auth_provider = PlainTextAuthProvider('ZJZOpmlgllBFRPgIsqRuRqlb', 'vyQMUx8ggESlZ890qk8+.I-YMRwww9bgG-34iO9qwnSx1,NHdic,Sgel8vPufQPXCGN7,_.WmU43R.5DL+y8+FLx7XA.3gydR15kkolTC+RH_bU74j,ncFN6HHR24a.a')

print("configurando cluster...")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

print("conectando com o banco!")
session = cluster.connect()

## Conexão com o banco ##
print("Usando Keyspace")
session.execute("USE mercadolivre")


## Criação de tabelas ##
print("\n Tabela de usuários criada")
session.execute("CREATE TABLE IF NOT EXISTS usuario (email text PRIMARY KEY, nome text, cpf text, endereco list<text>, favoritos list<text>);")

print("\n Tabela de vendedores criada")
session.execute("CREATE TABLE IF NOT EXISTS vendedor (email text PRIMARY KEY, nome text, cnpj text, end list<text>);")

print ("\n Tabela de produtos criada")
session.execute("CREATE TABLE IF NOT EXISTS produto (id text PRIMARY KEY, nome text, preco text, quantidade text, status text, vendedor list<text>);")

print ("\n Tabela de compras criada")
session.execute("CREATE TABLE IF NOT EXISTS compra (id text PRIMARY KEY, precototal text, status text, data text, formapagamento text, produto list<text>, vendedor list<text>, usuario list<text>);")


## Criação de usuários ##
def insertUsuario(email, cpf, endereco, nome):
      print("\n Usuário criado!")
      preInsert = session.prepare("INSERT INTO usuario (email, cpf, endereco, nome) VALUES (?, ?, ?, ?);")
      session.execute(preInsert, [email, cpf, endereco, nome])

#insertUsuario('mariana_tamay@gmail.com', '123.456.789.00',  ['Jardim das Indústrias', '3213', 'São José dos Campos', 'SP'], 'Mariana')

## Criação de vendedores ##
def insertVendedor(email, cnpj, end, nome):
      print("\n Vendedor criado!")
      preInsert = session.prepare("INSERT INTO vendedor (email, cnpj, end, nome) VALUES (?, ?, ?, ?);")
      session.execute(preInsert, [email, cnpj, end, nome])

#insertVendedor('priscila@email.com', '12.345.678/0001-90.', ['Zona Sul', '10', 'São José dos Campos', 'SP'], 'Priscila')

## Criação de produtos ##
def insertProduto(id, nome, preco, quantidade, status, vendedor):
      print("\n Produto criado!")
      vended = session.execute("SELECT * FROM vendedor;")
      for vend in vended:
            if (vend.email == vendedor):
                  vendedorInfo = [vendedor, vend.nome]
                  preInsert = session.prepare("INSERT INTO produto (id, nome, preco, quantidade, status, vendedor) VALUES (?, ?, ?, ?, ?, ?);")
                  session.execute(preInsert, [id, nome, preco, quantidade, status, vendedorInfo])

#insertProduto('1', 'API', '3000,00', '1', 'Disponível', 'priscila@email.com')

def insertCompra(id, data, formapagamento, quantidade, produtoId,  status, vendedorEmail, usuarioEmail):
    print ("\nCompra!")

    produtos = session.execute("SELECT * FROM produto;")

    vendedores = session.execute("SELECT * FROM vendedor;")

    usuarios = session.execute("SELECT * FROM usuario;")


    for produto in produtos:
        print("--->" ,produto)
        if (produto.id == produtoId):
            for vendedor in vendedores:
                print("--->" ,vendedor, vendedor.email, vendedorEmail)
                if (vendedor.email == vendedorEmail):
                    for usuario in usuarios:
                        print("--->" ,usuario)
                        if (usuario.email == usuarioEmail):
                            precototal = int(produto.preco) * quantidade
                            
                            preInsert = session.prepare("INSERT INTO compra (id, data, formapagamento, precototal, produto, status, vendedor, usuario) VALUES (?, ?, ?, ?, ?, ?, ?, ?);")
                            print("pre insert ", preInsert)
                            session.execute(preInsert, [id, data, formapagamento, str(precototal), [produtoId, produto.nome, produto.preco], status, [vendedorEmail, vendedor.nome], [usuarioEmail, usuario.nome]])

#insertCompra('1', '26.10.2022', 'Boleto', 2, '1', 'Finalizada!', 'priscila@email.com', 'mariana_tamay@gmail.com')

def insertFavoritos(email, produtoNome):
    print ("\nFavorito criado!")

    produtos = session.execute("SELECT * FROM produto;")
    usuarios = session.execute("SELECT * FROM usuario;")

    for usuario in usuarios:
        if (usuario.email == email):
            for produto in produtos:
                if (produto.nome == produtoNome):
                    preInsert = session.prepare("UPDATE usuario SET favoritos = favoritos + ? WHERE email = ?;")
            
                    session.execute(preInsert, [ [produto.id, produtoNome, produto.preco, produto.vendedor[0], produto.vendedor[1] ], email])

# insertFavoritos('mariana_tamay@gmail.com', 'API')


## READ ##
## Lista de todos os usuários ##
def readUsuarios():
    print ("\nTabela de usuários")

    usuarios = session.execute("SELECT * FROM usuario;")
    usuariosLista = [{}]

    for usuario in usuarios:
        usuariosLista.append({usuario.email, usuario.nome, usuario.endereco[0], usuario.endereco[1], usuario.endereco[2], usuario.endereco[3]})
        print (usuariosLista)

#readUsuarios()

## Busca de usuário ##
def readUsuario(email):
    print ("\n Buscar usuário")

    usuarios = session.execute("SELECT * FROM usuario;")
    usuarioLista = {}

    for usuario in usuarios:
        if (usuario.email == email):
            usuarioLista = {usuario.nome, usuario.cpf, usuario.endereco[0], usuario.endereco[1], usuario.endereco[2], usuario.endereco[3]}
        print (usuarioLista)

#readUsuario("mariana_tamay@gmail.com")

## Lista de vendedores ##
def readVendedores():
    print ("\n Lista de vendedores")

    vendedores = session.execute("SELECT * FROM vendedor;")
    vendedorLista = [{}]

    for vendedor in vendedores:
        vendedorLista.append({vendedor.email, vendedor.nome, vendedor.end[0], vendedor.end[1], vendedor.end[2], vendedor.end[3]})
        print (vendedorLista)

#readVendedores()

## Busca de vendedor ##
def readVendedor(email):
    print ("\nBuscar vendedor")

    vendedores = session.execute("SELECT * FROM vendedor;")
    vendedorList = {}

    for vendedor in vendedores:
        if (vendedor.email == email):
            vendedorList = {vendedor.nome, vendedor.cnpj, vendedor.end[0], vendedor.end[1], vendedor.end[2], vendedor.end[3]}
        print (vendedorList)

#readVendedor("priscila@email.com")

## Lista de produtos ##
def readProdutos():
    print ("\n Lista de produtos")
    produtos = session.execute("SELECT * FROM produto;")
    produtosLista = [{}]

    for produto in produtos:
        produtosLista.append({produto.nome, produto.preco, produto.quantidade, produto.status, produto.vendedor[0], produto.vendedor[1]})
        print (produtosLista)

#readProdutos()

## Busca de produto ##
def readProduto(id):
    print ("\n Buscar produto")

    produtos = session.execute("SELECT * FROM produto;")
    produtosLista = {}

    for produto in produtos:
        if (produto.id == id):
            produtosLista = {produto.nome, produto.preco, produto.quantidade, produto.status, produto.vendedor[0], produto.vendedor[1]}
            print (produtosLista)

#readProduto("1")

def readUsuarioFavoritos(email):
    print ("\nLista de favoritos")

    usuarios = session.execute("SELECT * FROM usuario;")

    for usuario in usuarios:
        if (usuario.email == email):
            favoritos = usuario.favoritos

            index = 0

            while (index < len(favoritos)):

                print (f'Id: {favoritos[index]}')
                print (f'Nome: {favoritos[index + 1]}')
                print (f'Preço: {favoritos[index + 2]}')
                print (f'Vendedor Email: {favoritos[index + 3]}')
                print (f'Vendedor Name: {favoritos[index + 4]}')

                print ("---")

                index += 5

#readUsuarioFavoritos('mariana_tamay@gmail.com')    

def readCompras():
    print ("\nLista de compras")

    compras = session.execute("SELECT * FROM compra;")
    comprasLista = [{}]

    for compra in compras:
        comprasLista.append({compra.id, compra.precototal, compra.status, compra.data, compra.formapagamento, compra.produto[0], compra.produto[1], compra.produto[2], compra.produto[3], compra.vendedor[0], compra.vendedor[1], compra.usuario[0], compra.usuario[1]})
    
    print (comprasLista)      

#readCompras()      

def readCompra(id):
    print ("\nBuscar compra")

    compras = session.execute("SELECT * FROM compra;")
    comprasLista = [{}]

    for compra in compras:
        if (compras.id == id):
            comprasLista = {compra.id, compra.precototal, compra.status, compra.data, compra.formapagamento, compra.produto[0], compra.produto[1], compra.produto[2], compra.produto[3], compra.vendedor[0], compra.vendedor[1], compra.usuario[0], compra.usuario[1]}
    
    print (comprasLista)

#readCompra("1")


## UPDATE ##
def updateUsuario(email, nome, cpf, endereco):
    print ("\nUsuário atualizado")

    usuarios = session.execute("SELECT * FROM usuario;")

    for usuario in usuarios:
        if (usuario.email == email):
            session.execute("UPDATE usuario SET cpf = '%s', endereco = ['%s', '%s', '%s', '%s'], nome = '%s' WHERE email = '%s'" % (cpf, endereco[0], endereco[1], endereco[2], endereco[3], nome, email))
            
            readUsuario(email)

#updateUsuario('mariana_tamay@gmail.com', 'Ayumi','111.222.333.44', ['Não me recordo', '27', 'Manaus', 'AM'])

def updateVendedor(email, cnpj, end, nome):
    print ("\n Editar vendedor")
    vendedores = session.execute("SELECT * FROM vendedor;")

    for vendedor in vendedores:
       if (vendedor.email == email):
            session.execute("UPDATE vendedor SET cnpj = '%s', end = ['%s', '%s', '%s', '%s'], nome = '%s'  WHERE email = '%s'" % (cnpj, end[0], end[1], end[2], end[3], nome, email))
            
            readVendedor(email)

#updateVendedor('priscila@email.com','999.888.777.66', ['Para', '25', 'sjc', 'SP'], 'priscilinha')

def updateProduto(id, nome, preco, quantidade, status, vendorEmail):
    print ("\n Editar produto")

    produtos = session.execute("SELECT * FROM produto;")

    vendedor = session.execute("SELECT * FROM vendedor;")

    for vendor in vendedor:
        if (vendor.email == vendorEmail):
            for product in produtos:
                if (product.id == id):
                    session.execute("UPDATE produto SET nome = '%s', preco = '%s', quantidade = '%s', status = '%s', vendedor = ['%s', '%s'] WHERE id = '%s'" % (nome, preco, quantidade, status, vendorEmail, vendor.nome, id))
                    
                    readProduto(id)

#updateProduto('1', 'Exercício BD', "60", '10', 'Disponível', 'priscila@email.com')


## DELETE ##
def deleteUsuario(email):
    print ("\n Deletar usuário")
    
    usuario = session.execute("SELECT * FROM usuario;")

    for user in usuario:
        if (user.email == email):
            session.execute("DELETE FROM usuario WHERE email = '%s'" % email)
            
            readUsuarios()

##deleteUsuario("mariana@gmail.com")

def deleteVendedor(email):
    print ("\n Deletar vendedor")

    vendedor = session.execute("SELECT * FROM vendedor;")

    for vendor in vendedor:
        if (vendor.email == email):
            session.execute("DELETE FROM vendedor WHERE email = '%s'" % email)
            
            readVendedores()

#deleteVendedor("priscilinha")

def deleteProduto(id):
    print ("\nDeletar produto")

    produtos = session.execute("SELECT * FROM produto;")

    for product in produtos:
        if (product.id == id):
            session.execute("DELETE FROM produto WHERE id = '%s'" % id)
            
            readProdutos()

#deleteProduto("1")

