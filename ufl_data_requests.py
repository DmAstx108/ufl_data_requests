import datetime
import requests
from backend.db_info import data_mongo, date_offset_aware, randomword
from config_file import url_ufl_api, collection_name_ufl_data
import ast
import json
from base64 import b64encode
import os


class UflData:
    def __init__(self):
        self.methods = {'getTeamNews': {'json_data': '''{"query":"{\\n  allNews(\\n    limit: 1000\\n    cursor: 0\\n    include: {\\n      teamId: $teamId,\\n    }\\n  ){\\n    total\\n    edges{\\n      id\\n      createdAt\\n      tags\\n      blocks{\\n        data\\n      }\\n    }\\n  }\\n}","variables":{}}''',
                                        'param': ['teamId']},
                        'getUser': {'json_data': '''{"query":"{\\\r\\\n  allTournamentTeamMembers(\\\r\\\n      query: {\\\r\\\n      filters: [  \\\r\\\n      { field: \\\"firstName\\\", value: \\\"$firstName\\\" },\\\r\\\n      { field: \\\"lastName\\\", value: \\\"$lastName\\\" },\\\r\\\n      { field: \\\"birthday\\\", value: \\\"$birthday\\\" },\\\r\\\n      ]\\\r\\\n    }\\\r\\\n  ){\\\r\\\n    edges{   \\\r\\\n      id\\\r\\\n      teamId\\\r\\\n      firstName\\\r\\\n      lastName\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                    'param': ['firstName', 'lastName', 'birthday']},
                        'getTeamNewsIds': {'json_data': '''{"query":"{\\\r\\\n  teamNews(\\\r\\\n    teamId: $teamId\\\r\\\n    limit: $limit\\\r\\\n    cursor: 0\\\r\\\n  ){\\\r\\\n   total\\\r\\\n    edges{\\\r\\\n      id\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                           'param': ['teamId', 'limit']},
                        'getAllNews': {'json_data': '''{"query":"{\\n  allNews(\\n    limit: 1000\\n    cursor: 0\\n    include: {\\n      tag: \\"$tagName\\"\\n    }\\n  ){\\n    total\\n    edges{\\n      id\\n      createdAt\\n      tags\\n      blocks{\\n        data\\n      }\\n    }\\n  }\\n}","variables":{}}''',
                                       'param': ['tagName']},
                        'getAllNewsIds': {'json_data': '''{"query":"{\\\r\\\n  allNews(\\\r\\\n  limit: $limit\\\r\\\n    cursor: 0\\\r\\\n  ){\\\r\\\n   total\\\r\\\n    edges{\\\r\\\n      id\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                          'param': ['limit']},
                        'allTournamentMatches': {'json_data': '''{"query":"{\\n  allTournamentMatches( \\n    query: { \\n      filters: [{ field: \\"seasonId\\", value: \\"$seasonId\\" }]\\n      limit: 10000\\n    }) {\\n    total\\n    cursor\\n    edges {\\n      id\\n      title\\n      startDate\\n      finishDate\\n      team1Id\\n      team2Id\\n      team1IdGoals\\n      team2IdGoals\\n      team1 {\\n        id\\n        name\\n        logo\\n      }\\n      team2 {\\n        id\\n        name\\n        logo\\n      }\\n    }\\n  }\\n}","variables":{}}''',
                                                 'param': ['seasonId', 'limit']},
                        'getRoutsMatchTournament': {'json_data': '''{"query":"{\\\r\\\n  allTournamentMatches(\\\r\\\n    query: { filters: [{ field: \\\"seasonId\\\", value: \\\"$seasonId\\\" }]\\\r\\\n    limit: $limit\\\r\\\n    }\\\r\\\n  ) {\\\r\\\n    total\\\r\\\n    cursor\\\r\\\n    edges {\\\r\\\n      id\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                                    'param': ['seasonId', 'limit']},
                        'getRoutsNewsTeam': {'json_data': '''{"query":"{\\\r\\\n  teamNews(teamId:$teamId, limit: $limit)\\\r\\\n  {\\\r\\\n    edges{\\\r\\\n      id\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                             'param': ['teamId', 'limit']},
                        'getRoutsNewsTournament': {'json_data': '''{"query":"{\\\r\\\n  allNews(limit: $limit)\\\r\\\n  {\\\r\\\n    edges{\\\r\\\n      id\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                                   'param': ['limit']},
                        'getTournamentMatchLineUps': {'json_data': '''{"query":"{\\n  tournamentMatchLineUps(\\n    query: {\\n      limit : $limit,\\n      filters: [\\n        { field: \\"matchId\\", value: \\"$matchId\\" }\\n        { field: \\"teamId\\", value: \\"$teamId\\" }\\n      ]\\n    }\\n  ) {\\n    id\\n    matchId\\n    teamId\\n    number\\n    initialRole\\n    initialState\\n    teamMemberId\\n    type\\n    teamMember{\\n      firstName\\n      lastName\\n    }\\n  }\\n}","variables":{}}''',
                                                      'param': ['limit', 'matchId', 'teamId']},
                        'getTournamentMatchEvents': {'json_data': '''{"query":"{\\\r\\\n  allTournamentMatchEvents(\\\r\\\n    query: {\\\r\\\n      limit: $limit,\\\r\\\n      filters: [\\\r\\\n        { field: \\\"isProtocol\\\", value: \\\"true\\\" },\\\r\\\n        { field: \\\"matchId\\\", value: \\\"$matchId\\\" }\\\r\\\n      ]\\\r\\\n    }\\\r\\\n  ) {\\\r\\\n    total\\\r\\\n    edges {\\\r\\\n      id\\\r\\\n      event\\\r\\\n      teamId\\\r\\\n     minute\\\r\\\n    player{\\\r\\\n        firstName\\\r\\\n      lastName\\\r\\\n}\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
                                                     'param': ['limit', 'matchId']},
                        'getTournamentMatch': {'json_data': '''{"query":"{\\n  tournamentMatch(id: $matchId){\\n    id\\n    title\\n    startDate\\n    finishDate\\n    team1Id\\n    team2Id\\n    team1IdGoals\\n    team2IdGoals\\n    team1{\\n      id\\n      logo\\n      name\\n    }\\n    team2{\\n      id\\n      logo\\n      name\\n    }\\n  }\\n}","variables":{}}''',
                                               'param': ['matchId']},
                        'getUserId': {'json_data': '''{"query":"{\\n  tournamentTeamMembers(\\n    query: {\\n    filters: [\\n      { field: \\"firstName\\", value: \\"$firstName\\"}\\n      { field: \\"lastName\\", value: \\"$lastName\\"}\\n      { field: \\"birthday\\", value: \\"$birthday\\"}\\n    ]\\n  }\\n  ){\\n    id\\n  }\\n}","variables":{}}''',
                                      'param': ['firstName', 'lastName', 'birthday']},
                        'getTeamAndSeasonId': {'json_data': '''{"query":"{\\n  tournamentSeasonTeamMembers(query: {\\n    filters: [\\n      { field: \\"teamMemberId\\", value: \\"$teamMemberId\\"}\\n    ]\\n  }) {\\n    teamId\\n    seasonId\\n    teamMemberId\\n  }\\n}","variables":{}}''',
                                               'param': ['teamMemberId']},
                        'getTournamentPlayerStats': {'json_data': '''{"query":"{\\n  allTournamentPlayerStats(\\n    query: {\\n      filters: [\\n        { field: \\"playerId\\", value: \\"$playerId\\"}\\n      ]\\n    }) {\\n    edges{\\n      id\\n      playerId\\n      seasonId\\n      penaltyKicks\\n      games\\n      goals\\n      passes\\n      yellowCards\\n      redCards\\n      missedGoals\\n      shutouts\\n      position\\n      bombardierValue\\n      assistantValue\\n      goalsAndPassesValue\\n      goalkeeperValue\\n      warningValue\\n      allGoals\\n      goalsAndPasses\\n      avgGoals\\n      avgPasses\\n      createdAt\\n      updatedAt\\n    }\\n  }\\n}","variables":{}}''',
                                                     'param': ['playerId']},
                        'getTournamentMatchStats': {'json_data': '''{"query":"{\\n  tournamentMatchStats(\\n        query: {\\n        \\n    filters: [\\n      { field: \\"matchId\\", value: \\"$matchId\\"}\\n    ]\\n  }\\n  ){\\n    id\\n    matchId\\n    team1Id\\n    team2Id\\n    type\\n    team1Value\\n    team2Value\\n    team1Comparative\\n    team2Comparative\\n    createdAt\\n  }\\n}","variables":{}}''',
                                                    'param': ['matchId']},
                        'checkNews':
                        ''}

    def upd_methdos(self, meth_name, args_request):

        if 'fast' in args_request:
            args_request = args_request['fast']
            if 'session_code' in args_request and \
                    not str(data_mongo.check_session_key_in_db(args_request['session_code'], add_param=True)) in ['-1', '310'] and \
                    meth_name in self.methods:
                del args_request['session_code']
                check_n_1 = self.methods[meth_name]['param']
                check_n_2 = not False in [isinstance(
                    i2, str) for i2 in args_request.values()]
                if len(list(args_request)) == len(check_n_1) and\
                    not False in [i in check_n_1 for i in args_request]\
                        and check_n_2:
                    args_request['meth_name'] = meth_name
                    if 'limit' in args_request and args_request['meth_name'] in ['allTournamentMatches',
                                                                                 'tournamentMatchLineUps',
                                                                                 'allTournamentMatchEvents']:
                        args_request['limit'] = '1000'
                    else:
                        args_request['limit'] = '100'

                    print('search_info_start', meth_name, args_request)

                    prom_n = data_mongo.db[collection_name_ufl_data].find_one(
                        args_request)
                    print('search_info_end', meth_name, args_request, prom_n)

                    if prom_n:
                        print('find_all', prom_n)
                        if (date_offset_aware() - prom_n['datetime_n']).days > 3:
                            print('passtime', (date_offset_aware() -
                                  prom_n['datetime_n']).seconds > 15 * 60)

                            try:
                                print('check_methods', meth_name !=
                                      'checkNews', meth_name)
                                if meth_name != 'checkNews':
                                    print('insert_meth', args_request)

                                    str_req = self.methods[meth_name]['json_data']
                                    for i, j in args_request.items():
                                        str_req = str_req.replace(
                                            f'${i}', str(j))
                                    o = ast.literal_eval(str_req)
                                    file_name = f"{randomword(60)}.json"
                                    with open(file_name, 'w') as fp:
                                        json.dump(o, fp)
                                    headers = {
                                        'Content-Type': 'application/json'
                                    }
                                    rez = requests.post(url_ufl_api, data=open(
                                        file_name, 'rb'), headers=headers)
                                else:
                                    print('start_request_online')
                                    rez = requests.get(args_request['url'])
                                    print('end_request_online')

                                if rez.status_code == 200:
                                    # try:
                                    #     os.remove(file_name)
                                    # except:
                                    #     pass
                                    if meth_name != 'checkNews':
                                        data_rez = rez.json()
                                        print('strart_update_one_db',
                                              args_request)
                                        data_mongo.db[collection_name_ufl_data].update_one(args_request,
                                                                                           {'$set': {'data_rez': data_rez,
                                                                                                     'datetime_n': date_offset_aware()}})
                                        print('end_update_one_db',
                                              args_request)

                                        return data_rez

                                    else:
                                        data_rez = {'data_rez': rez.text}
                                        # test_answer = {'res': data_rez}
                                        data_mongo.db[collection_name_ufl_data].update_one(args_request,
                                                                                           {'$set': {'data_rez': data_rez,
                                                                                                     'datetime_n': date_offset_aware()}})

                                        return data_rez
                                else:
                                    return prom_n['data_rez']
                            except Exception as e:
                                print('update data ufl', e, prom_n['data_rez'])

                                return prom_n['data_rez']
                        else:
                            return prom_n['data_rez']
                    else:
                        print('not_found', prom_n)

                        try:
                            print('check_methods_1', meth_name !=
                                  'checkNews', meth_name)

                            if meth_name != 'checkNews':
                                print('insert_meth', args_request)

                                str_req = self.methods[meth_name]['json_data']
                                for i, j in args_request.items():
                                    str_req = str_req.replace(f'${i}', str(j))
                                o = ast.literal_eval(str_req)
                                file_name = f"{randomword(60)}.json"
                                with open(file_name, 'w') as fp:
                                    json.dump(o, fp)
                                headers = {
                                    'Content-Type': 'application/json'
                                }

                                rez = requests.post(url_ufl_api, data=open(
                                    file_name, 'rb'), headers=headers)
                            else:
                                print('start_parsing', args_request['url'])
                                rez = requests.get(args_request['url'])
                                print('end_parsing', args_request['url'])
                            if rez.status_code == 200:
                                if meth_name != 'checkNews':
                                    data_rez = rez.json()
                                else:
                                    data_rez = {'data_rez': rez.text}
                                args_request['data_rez'] = data_rez
                                args_request['datetime_n'] = datetime.datetime.now(
                                )
                                print('strart_insert_one_db', args_request)

                                data_mongo.db[collection_name_ufl_data].insert_one(
                                    args_request)
                                print('strart_end_one_db', args_request)

                                # try:
                                #     os.remove(file_name)
                                # except:
                                #     pass
                                return data_rez
                            else:
                                return {}
                        except Exception as e:
                            print('error insert data ufl', e)
                            return {}
                else:
                    return '331'
            else:
                return '330'
        else:
            return '404'

    # def add_routes(self):
    #     for i in self.methods:
    #         app.add_route(i, )
# wsw = UflData()
# body = '''{"query":"{\\\r\\\n  tournamentMatch(id: $matchId){\\\r\\\n    id\\\r\\\n    title\\\r\\\n    startDate\\\r\\\n    finishDate\\\r\\\n    team1Id\\\r\\\n    team2Id\\\r\\\n    team1IdGoals\\\r\\\n    team2IdGoals\\\r\\\n    season{\\\r\\\n      id\\\r\\\n      title\\\r\\\n    }\\\r\\\n    team1{\\\r\\\n      id\\\r\\\n      logo\\\r\\\n      name\\\r\\\n    }\\\r\\\n    team2{\\\r\\\n      id\\\r\\\n      logo\\\r\\\n      name\\\r\\\n    }\\\r\\\n  }\\\r\\\n}","variables":{}}''',
# payload_1 = {"fast": {
#   "limit": "1000",
#   "seasonId": '1024268',
#   "session_code": "ZSThWk/pr14BATKz2ZCWevR25x6iQKMBO01GMV2F8KGcX8U+A6HK9G+6cftKfztTq5rOccO6idYY9V6N2pijBp5Vo6AzHWHuewM5ciafvPjH5niBdEZKLjcElQaG0gkmq+e4Tagav215hyVYOvOyMSQwoq0m3hEErt82jxhcW9A=",
# }}
UflData_client = UflData()
# print('rez_data',UflData_client.upd_methdos('allTournamentMatches', payload_1))
# print('rez_data', UflData_client.upd_methdos('getTeamNews', {
# "fast": {
#     "teamId": '1247150',
#     "session_code": "S48GNk94bdWF7LfMpEIxAxqepiPsnjpP+Qy78u7xDZbNRDqeyq5gku5MTLmdsb6YHkfkw49PNlGM5iSPerV/W3uNR6BvuzDQ22InMu2Ts++58So4l9W0b9ZJW02TFSYQrH7WJsLvhX+Y6yqeBwDWa6PRtbMXTC1ms1xk9DypWEA="
#     }
# }))
# # print(len(wsw.methods))
# te = requests.get('https://api.test.yflrussia.ru/graphql')
# print(te)
