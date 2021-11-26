#

# IMPORTAR MÓDULOS
from re import I
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
import json

# CONFIGURAR CONEXÃO COM BANCO DE DADOS SQLITE
engine = create_engine("sqlite:///server.db")
connection = engine.connect()

# INICIAR SESSÃO COM BANCO DE DADOS
session = Session()

# INSTANCIAR CLASSE BASE DO SQLALCHEMY
Base = declarative_base(engine)


# Classe para mapeamento da tabela
class Filme(Base):

    # FAZER AQUI O MAPEAMENTO DA TABELA
    __tablename__ = 'FILME'
    id = Column('ID', Integer, primary_key=True, autoincrement=True)
    titulo = Column('TITULO', String(255))
    ano = Column('ANO', Integer)
    genero = Column('GENERO', String(255))
    duracao = Column('DURACAO', Integer)
    pais = Column('PAIS', String(255))
    diretor = Column('DIRETOR', String(255))
    elenco = Column('ELENCO', String(255))
    avaliacao = Column('AVALIACAO', Float)
    votos = Column('VOTOS', Integer)

    # Método construtor
    def __init__(self, titulo, ano, genero, duracao, pais, diretor, elenco, avaliacao, votos):
        self.titulo = titulo
        self.ano = ano
        self.genero = genero
        self.duracao = duracao
        self.pais = pais
        self.diretor = diretor
        self.elenco = elenco
        self.avaliacao = avaliacao
        self.votos = votos


# Classe para interação com o Banco de Dados
class BancoDeDados:
    def criar_tabela(self):
        # Cria a tabela FILME no banco de dados
        connection.execute("""CREATE TABLE IF NOT EXISTS FILME(
                              ID INTEGER PRIMARY KEY,
                              TITULO VARCHAR(255),
                              ANO INT,
                              GENERO VARCHAR(255),
                              DURACAO INT,
                              PAIS VARCHAR(255),
                              DIRETOR VARCHAR(255),
                              ELENCO VARCHAR(255),
                              AVALIACAO FLOAT,
                              VOTOS INT)""")

    def incluir(self, filme):
        session.add(filme)
        session.commit()

    def incluir_lista(self, filmes):
        session.add_all(filmes)
        session.commit()

    def alterar_avaliacao(self, id_filme, avaliacao):
        filme = session.query(Filme).get(id_filme)
        if filme is not None:
            filme.avaliacao = avaliacao
            session.commit()

    def excluir(self, id_filme):
        filme = session.query(Filme).get(id_filme)
        if filme is not None:
            session.delete(filme)
            session.commit()

    def buscar_todos(self):
        filmes = session.query(Filme).order_by(Filme.titulo)
        return filmes

    def buscar_por_ano(self, ano):
        filmes = session.query(Filme).filter(Filme.ano == ano).order_by(Filme.ano)
        return filmes

    def buscar_por_genero(self, genero):
        filmes = session.query(Filme).filter(Filme.genero.like('%' + genero + '%')).order_by(Filme.titulo)
        return filmes

    def buscar_por_elenco(self, ator):
        filmes = session.query(Filme).filter(Filme.elenco.like('%' + ator + '%')).order_by(Filme.ano)
        return filmes

    def buscar_melhores_do_ano(self, ano):
        filmes = session.query(Filme).filter(Filme.ano == ano and Filme.avaliacao >= 90).order_by(Filme.avaliacao)
        return filmes

    def exportar_filmes(self, nome_arquivo):
        data = {}
        resultado = session.query(Filme).order_by(Filme.titulo)
        for f in resultado:
            data[f.titulo] = {'Ano': f.ano, 'Gênero': f.genero, 'Duração': f.duracao, 'País': f.pais, 'Diretor': f.diretor,
                            'Elenco': f.elenco, 'Avaliação': f.avaliacao, 'Votos': f.votos}
        with open(nome_arquivo, 'w', encoding='UTF-8') as saida:
            json.dump(data, saida, indent=4, ensure_ascii=False)

    def importar_filmes(self, nome_arquivo):
        arquivo = open(nome_arquivo, 'r', encoding='UTF-8')
        lista_filmes = []
        for linha in arquivo:
            lista = linha.split(';')
            filme = Filme(lista[0], int(lista[1]), lista[2], int(lista[3]), lista[4], lista[5], lista[6],
                          float(lista[7]), int(lista[8]))
            lista_filmes.append(filme)
        session.add_all(lista_filmes)
        session.commit()


# EXEMPLO DE PROGRAMA PRINCIPAL
banco = BancoDeDados()
banco.criar_tabela()

# Importa filmes do arquivo movies.txt e salva no banco de dados
banco.importar_filmes('movies.txt')

# Cria um novo Filme e insere no banco de dados
filme1 = Filme("Parasite", 2019, "Comedy, Drama, Thriller", 132, "Korea",
               "Bong Joon Ho", "Song Kang-ho, Jang Hye-jin, Choi Woo-shik", 92, 40273)
banco.incluir(filme1)

# Cria uma lista com dois novos filmes e insere no banco de dados
filme2 = Filme("Joker", 2019, 'Crime, Drama, Thriller', 122, "USA",
               "Todd Phillips", "Joaquin Phoenix, Robert De Niro, Zazie Beetz", 91, 78481)
filme3 = Filme("Avengers: Endgame", 2019, 'Drama, Thriller', 181, "USA",
               "Anthony Russo, Joe Russo", "Robert Downey Jr., Chris Evans, Mark Ruffalo", 93, 715250)
lista_filmes = [filme2, filme3]
banco.incluir_lista(lista_filmes)

# Altera a avalação do filme de id 7 para 98
banco.alterar_avaliacao(7, 98)

# Exclui o filme de id 6
banco.excluir(6)

# Busca todos os filmes
lista = banco.buscar_todos()
print('-'*60)
for f in lista:         # exibe lista de filmes
    print(f.id, f.titulo, f.ano, f.genero, f.diretor, f.elenco, f.avaliacao)

# Busca todos os filmes do ano de 2019
lista = banco.buscar_por_ano(2019)
print('-'*60)
for f in lista:         # exibe lista de filmes
    print(f.id, f.titulo, f.ano)


# Busca todos os filmes do gênero 'Crime'
lista = banco.buscar_por_genero('Crime')
print('-'*60)
for f in lista:         # exibe lista de filmes
    print(f.id, f.titulo, f.ano, f.genero)


# Busca todos os filmes com participação da atriz de nome 'Nicole Balsam'
lista = banco.buscar_por_elenco('Nicole Balsam')
print('-'*60)
for f in lista:         # exibe lista de filmes
    print(f.id, f.titulo, f.ano, f.elenco)


# Busca os melhores filmes do ano de 2019
lista = banco.buscar_melhores_do_ano('2019')
print('-'*60)
for f in lista:         # exibe lista de filmes
    print(f.id, f.titulo, f.ano, f.avaliacao)


# Exporta filmes do banco de dados para um novo arquivo de texto
banco.exportar_filmes('outfile.json')
