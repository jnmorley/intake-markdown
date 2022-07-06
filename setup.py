from setuptools import setup, find_packages

setup(
    entry_points={
        'intake.drivers': [
            'markdown = intake_markdown.intake_markdown:MarkdownSource',
        ]
    },
)