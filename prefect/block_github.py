import os
from dotenv import load_dotenv
from prefect.filesystems import GitHub


if __name__ == '__main__':
    
    load_dotenv()

    repo = os.getenv('GITHUB_REPOSITORY')

    block = GitHub(
        name="de-zoomcamp-project",
        repository=repo
    )

    block.save('de-zoomcamp-project', overwrite=True)
