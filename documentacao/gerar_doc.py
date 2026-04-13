"""
Gera o documento de arquitetura AV1 em PDF.
Execute a partir da raiz do projeto:
    python documentacao/gerar_doc.py
"""

import io
from pathlib import Path

import cairosvg
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "documentacao" / "arquitetura_av1.pdf"

# ---------------------------------------------------------------------------
# Estilos
# ---------------------------------------------------------------------------
styles = getSampleStyleSheet()

TITLE = ParagraphStyle(
    "title",
    parent=styles["Title"],
    fontSize=22,
    leading=28,
    spaceAfter=6,
    textColor=colors.HexColor("#1a1a2e"),
)
H1 = ParagraphStyle(
    "h1",
    parent=styles["Heading1"],
    fontSize=14,
    leading=18,
    spaceBefore=18,
    spaceAfter=6,
    textColor=colors.HexColor("#16213e"),
    borderPad=(0, 0, 4, 0),
)
H2 = ParagraphStyle(
    "h2",
    parent=styles["Heading2"],
    fontSize=12,
    leading=16,
    spaceBefore=12,
    spaceAfter=4,
    textColor=colors.HexColor("#0f3460"),
)
BODY = ParagraphStyle(
    "body",
    parent=styles["Normal"],
    fontSize=10,
    leading=15,
    spaceAfter=6,
)
SMALL = ParagraphStyle(
    "small",
    parent=styles["Normal"],
    fontSize=9,
    leading=13,
    textColor=colors.HexColor("#444444"),
)

TABLE_HEADER = colors.HexColor("#16213e")
TABLE_ALT = colors.HexColor("#f0f4ff")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def hr():
    return HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc"), spaceAfter=4)


def svg_to_image(svg_path: Path, width_cm: float = 15) -> Image:
    """Converte SVG para PNG em memória e retorna um flowable Image."""
    target_px = int(width_cm / 2.54 * 150)  # 150 dpi
    png_bytes = cairosvg.svg2png(url=str(svg_path), output_width=target_px)
    buf = io.BytesIO(png_bytes)
    img = Image(buf)
    aspect = img.imageHeight / img.imageWidth
    img.drawWidth = width_cm * cm
    img.drawHeight = width_cm * cm * aspect
    return img


def tech_table(rows):
    """Cria tabela de tecnologias com cabeçalho estilizado."""
    header = [
        Paragraph("<b>Tecnologia</b>", SMALL),
        Paragraph("<b>Uso no projeto</b>", SMALL),
        Paragraph("<b>Alternativa paga</b>", SMALL),
        Paragraph("<b>Justificativa da escolha</b>", SMALL),
    ]
    table_rows = [header]
    for r in rows:
        table_rows.append([Paragraph(c, SMALL) for c in r])

    col_widths = [3.2 * cm, 4.5 * cm, 3.8 * cm, 5.5 * cm]
    t = Table(table_rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), TABLE_HEADER),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, TABLE_ALT]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bbbbbb")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return t


def team_table(rows):
    header = [
        Paragraph("<b>Membro</b>", SMALL),
        Paragraph("<b>Responsabilidades</b>", SMALL),
        Paragraph("<b>Contribuição no repositório</b>", SMALL),
    ]
    table_rows = [header] + [[Paragraph(c, SMALL) for c in r] for r in rows]
    col_widths = [4.5 * cm, 8.0 * cm, 4.5 * cm]
    t = Table(table_rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), TABLE_HEADER),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, TABLE_ALT]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bbbbbb")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return t


# ---------------------------------------------------------------------------
# Conteúdo
# ---------------------------------------------------------------------------

def build_story():
    story = []

    # Capa
    story.append(Spacer(1, 1.5 * cm))
    story.append(Paragraph("movies_bigdata", TITLE))
    story.append(Paragraph("Documento de Arquitetura — AV1", H1))
    story.append(hr())
    story.append(Spacer(1, 0.3 * cm))
    story.append(Paragraph(
        "Pipeline de engenharia e análise de dados para processamento de filmes "
        "a partir de arquivos CSV brutos, com consolidação de um dataset final, "
        "geração de tabelas analíticas e exportação de visualizações.",
        BODY,
    ))
    story.append(Paragraph("Data de entrega: 13/04/2026", SMALL))
    story.append(Spacer(1, 0.5 * cm))

    # 1. Diagrama
    story.append(Paragraph("1. Diagrama do Pipeline", H1))
    story.append(hr())
    story.append(Paragraph(
        "O diagrama abaixo representa o fluxo completo de dados, desde as fontes brutas "
        "até a publicação dos artefatos finais, organizado nas camadas "
        "<b>Bronze</b> (ingestão), <b>Silver</b> (limpeza e enriquecimento) e "
        "<b>Gold</b> (dataset consolidado e agregações).",
        BODY,
    ))
    story.append(Spacer(1, 0.3 * cm))
    svg_path = ROOT / "pipeline_diagrama.svg"
    if svg_path.exists():
        story.append(svg_to_image(svg_path, width_cm=16))
    else:
        story.append(Paragraph(f"[Diagrama não encontrado em {svg_path}]", SMALL))
    story.append(Spacer(1, 0.4 * cm))

    # 2. Arquitetura implementada
    story.append(Paragraph("2. Arquitetura Parcial Implementada", H1))
    story.append(hr())
    story.append(Paragraph(
        "Todo o pipeline está implementado em <b>codigo/pipeline_filmes.py</b>, "
        "orquestrado pela função <b>main()</b>. O notebook "
        "<b>notebooks/pipeline_filmes.ipynb</b> importa as funções individuais e "
        "permite inspecionar resultados intermediários por etapa. "
        "A arquitetura segue o modelo medallion (Bronze → Silver → Gold):",
        BODY,
    ))

    story.append(Paragraph("Camada Bronze — Ingestão e validação", H2))
    story.append(Paragraph(
        "• <b>load_raw_datasets()</b>: localiza dados/raw/, valida arquivos e colunas obrigatórias, "
        "carrega 5 CSVs (movies_metadata, credits, keywords, links, ratings_small).",
        BODY,
    ))

    story.append(Paragraph("Camada Silver — Limpeza e enriquecimento", H2))
    story.append(Paragraph(
        "• <b>clean_movies()</b>: remove IDs inválidos/duplicados, coerce colunas numéricas, "
        "elimina conteúdo adulto, trata orçamento/receita zero como nulos.<br/>"
        "• <b>build_directors()</b>: extrai diretor do campo crew via ast.literal_eval.<br/>"
        "• <b>build_keywords()</b>: extrai lista de palavras-chave e a palavra primária.<br/>"
        "• <b>build_movie_ratings()</b>: cruza ratings_small com links via movieId → tmdbId.",
        BODY,
    ))

    story.append(Paragraph("Camada Gold — Dataset final e agregações", H2))
    story.append(Paragraph(
        "• <b>build_gold_dataset()</b>: une os quatro resultados acima, deriva profit, roi, "
        "release_year, decade, has_collection.<br/>"
        "• <b>build_aggregations()</b>: produz 11 tabelas analíticas (receita por gênero, "
        "top diretores, tendências por década, keywords, correlações, top filmes).<br/>"
        "• <b>save_outputs()</b>: exporta CSV, Parquet (pyarrow) e CSVs por tabela.<br/>"
        "• <b>plot_results()</b>: gera 4 gráficos PNG com matplotlib/seaborn.",
        BODY,
    ))

    story.append(PageBreak())

    # 3. Tecnologias
    story.append(Paragraph("3. Tecnologias", H1))
    story.append(hr())
    story.append(Paragraph(
        "A tabela abaixo lista as ferramentas utilizadas, seu papel no projeto e "
        "as alternativas pagas que poderiam ser adotadas em um ambiente de produção escalável.",
        BODY,
    ))
    story.append(Spacer(1, 0.3 * cm))

    tech_rows = [
        ["pandas", "Leitura, limpeza, merge e agregação dos DataFrames", "Databricks (Spark)",
         "Gratuito e suficiente para o volume atual (~100 k linhas). Spark seria necessário apenas em escala de bilhões de registros."],
        ["matplotlib + seaborn", "Geração dos 4 gráficos analíticos em PNG", "Tableau / Power BI",
         "Bibliotecas open-source integradas ao pipeline Python sem custo adicional. BI pago agrega interatividade, mas não é necessário para entrega em imagem."],
        ["pyarrow", "Exportação do dataset gold em formato Parquet", "Delta Lake (Databricks)",
         "pyarrow é gratuito e viabiliza leitura eficiente em coluna. Delta Lake adiciona ACID e versionamento, mas exige infraestrutura Databricks."],
        ["Jupyter Notebook", "Exploração interativa e visualização passo a passo", "Databricks Notebooks / Google Colab Pro",
         "Jupyter é gratuito e local. Colab/Databricks oferecem GPUs e colaboração, mas introduzem custo e dependência de nuvem."],
        ["Python 3.11 + ast", "Parsing de campos JSON-like (crew, keywords, genres)", "Apache Spark + SparkSQL",
         "ast.literal_eval é adequado para os dados atuais. Spark seria necessário para paralelismo em datasets muito maiores."],
        ["GitHub", "Versionamento de código e artefatos", "GitLab Enterprise / Azure DevOps",
         "GitHub gratuito atende o fluxo do grupo. Versões pagas adicionam CI/CD avançado e controle de acesso corporativo."],
    ]
    story.append(tech_table(tech_rows))
    story.append(Spacer(1, 0.5 * cm))

    # 4. Checklist
    story.append(Paragraph("4. Estado Atual do Pipeline", H1))
    story.append(hr())

    checklist_rows = [
        [Paragraph("<b>Etapa</b>", SMALL), Paragraph("<b>Status</b>", SMALL), Paragraph("<b>Observação</b>", SMALL)],
        [Paragraph("Ingestão", SMALL), Paragraph("✓ Finalizado", SMALL),
         Paragraph("5 arquivos CSV validados por colunas e carregados via load_raw_datasets()", SMALL)],
        [Paragraph("Armazenamento", SMALL), Paragraph("✓ Finalizado", SMALL),
         Paragraph("Saída em CSV e Parquet (pyarrow) em dados/processed/", SMALL)],
        [Paragraph("Transformação", SMALL), Paragraph("✓ Finalizado", SMALL),
         Paragraph("Limpeza, enriquecimento, dataset gold e 11 tabelas agregadas", SMALL)],
        [Paragraph("Visualização", SMALL), Paragraph("✓ Finalizado", SMALL),
         Paragraph("4 gráficos PNG gerados em dados/processed/visualizacoes/", SMALL)],
    ]
    cl_widths = [4 * cm, 3.5 * cm, 9.5 * cm]
    cl_table = Table(checklist_rows, colWidths=cl_widths)
    cl_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), TABLE_HEADER),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, TABLE_ALT]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bbbbbb")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(cl_table)
    story.append(Spacer(1, 0.5 * cm))

    # 5. Equipe
    story.append(Paragraph("5. Equipe e Divisão de Tarefas", H1))
    story.append(hr())

    team_rows = [
        ["Theomilll\n(theo.alb.moura@gmail.com)",
         "Criação do repositório e estrutura inicial do projeto",
         "Commit inicial da base do código"],
        ["João Pedro Araújo Nóbrega\n(JoaoPNobrega)",
         "Documentação: README completo com descrição, estrutura, etapas da pipeline e requisitos",
         "Add comprehensive README for movie data pipeline"],
        ["menex100",
         "Upload dos arquivos de dados brutos e artefatos processados para o repositório",
         "Add files via upload (dados/raw, Parquet, PNGs)"],
        ["João Batista",
         "Refatoração do notebook para execução passo a passo com importações individuais "
         "das funções do pipeline; validação no Google Colab",
         "Refatora notebook para execucao passo a passo"],
    ]
    story.append(team_table(team_rows))

    return story


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(OUTPUT),
        pagesize=A4,
        leftMargin=2.5 * cm,
        rightMargin=2.5 * cm,
        topMargin=2.5 * cm,
        bottomMargin=2.5 * cm,
        title="movies_bigdata — Documento de Arquitetura AV1",
        author="Grupo movies_bigdata",
    )
    doc.build(build_story())
    print(f"PDF gerado: {OUTPUT}")


if __name__ == "__main__":
    main()
