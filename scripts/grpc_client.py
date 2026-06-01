import asyncio

from grpclib.client import Channel

from vvvvgross.catalog_category_api.v1.catalog_category_api_grpc import CatalogCategoryApiServiceStub
from vvvvgross.catalog_category_api.v1.catalog_category_api_pb2 import DescribeCategoryV1Request

async def main():
    async with Channel('127.0.0.1', 8082) as channel:
        client = atalogCategoryApiServiceStub(channel)

        req = DescribeCategoryV1Request(category_id=1)
        reply = await client.DescribeCategoryV1(req)
        print(reply.message)


if __name__ == '__main__':
    asyncio.run(main())
