from aiohttp import web
import nest_asyncio
import sqlalchemy
import time
import hofner as hf
import json
from aiohttp_index import IndexMiddleware
nest_asyncio.apply()
SQLALCHEMY_DATABASE_URI = 'postgresql://{user}:{password}@{host}/{database}'.format(
        host='postgres', 
	    port=5432, 
	    user='postgres', 
	    password='ovFPCzqbDN2XI0Ax', 
	    database='nyt')
    
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, echo=False)

async def handleCleanTableEvent(request): ### Function that handles clean event from api call using url (host/data/clean)
    published_date = request.rel_url.query['date']
    published_date=time.strptime(published_date, '%Y-%M-%d')
    table_name="_"+time.strftime('%Y_%M_%d',published_date)+"_bestsellers"
    hf.table_name="_"+time.strftime('%Y_%M_%d',published_date)+"_bestsellers"
    hf.create_table_with_published_date_data(published_date,engine)
    hf.fix_column_inconsistancy("title",table_name,engine)
    hf.fix_column_inconsistancy("author",table_name,engine)
    hf.fix_column_inconsistancy("description",table_name,engine)
    hf.response_obj = {"date":published_date}
    response_object=hf.get_table_date(table_name,engine)
    #print(response_object)
    return web.json_response(response_object)

app = web.Application(middlewares=[IndexMiddleware()])
app.router.add_get('/data/clean', handleCleanTableEvent)
app.router.add_static('/', 'static')


web.run_app(app)