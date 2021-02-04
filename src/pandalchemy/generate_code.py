from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlacodegen.codegen import CodeGenerator
from importlib import import_module


def main():
    connection_string = 'sqlite:///chinook.db'
    engine = create_engine(connection_string)
    code_name = 'chinook_models_nojoined.py'
    generate_code_file(code_name, engine, nojoined=True)
    import_code(code_name)


def generate_code(engine, **kwargs):
    """ CodeGenerator.__init__(self, metadata, noindexes=False, noconstraints=False,
                 nojoined=False, noinflect=False, nobackrefs=False,
                 flask=False, ignore_cols=None, noclasses=False, nocomments=False)
    """
    metadata = MetaData()
    metadata.reflect(bind=engine)
    codegen = CodeGenerator(metadata, **kwargs)
    sio = StringIO()
    codegen.render(sio)
    return sio.getvalue()


def generate_file(file_name, text):
    with open(file_name, 'w') as text_file:
        text_file.write(text)


def generate_code_file(file_name, engine, **kwargs):
    generate_file(file_name, generate_code(engine, **kwargs))


def import_code(file_name):
    import_module(file_name)

if __name__ == '__main__':
    main()
