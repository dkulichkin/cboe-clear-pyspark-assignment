FROM spark:3.5.0-python3
COPY dist .
COPY resources ../resources