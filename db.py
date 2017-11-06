
import aiofiles

# todo refactoring and configurable


SUCCESS_STORAGE = None


def get_domains_for_crawling(limit: int) -> list:
    out = []
    with open('data/success.txt') as success_index:
        success_domains = set(map(lambda x: x.strip(), success_index.readlines()))
        print(success_domains)
        with open('data/alexatop-1m.csv') as f:
            for row in f.readlines():
                rank, domain = int(row.split(',')[0]), row.split(',')[1].strip()
                if domain in success_domains:
                    print(domain + ' already parsed')
                    continue
                out.append((rank, domain))
                if len(out) > limit:
                    break
    return out


async def save_humans_txt(domain, rank, humans_content, response_code):
    global SUCCESS_STORAGE
    if is_success_response(response_code):
        if not SUCCESS_STORAGE:
            SUCCESS_STORAGE = await aiofiles.open('data/success.txt', mode='a')
        await SUCCESS_STORAGE.write(domain + "\n")
    # todo release
    pass


def is_success_response(code):
    return code >= 200 and code < 300