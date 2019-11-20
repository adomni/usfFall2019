import boto3
#from credentials import access_key, secret_key, region_name
from time import sleep
import os
from multiprocessing import Process
from threading import Thread
import pandas as pd
from datetime import datetime
import time
import sys
import itertools

#things to do prepare ml dataset and higher quality mobile devices
#also star tworking on aws lambda
#athena client init
#future
from boto3.session import Session
from boto3.dynamodb.conditions import Key
pd.set_option('display.max_columns', 7)
# dynamodb_session = Session(aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region_name)
#
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table('high_quality')
#
# table.put_item(Item = {'billboard_id': 'test', 'audience_segment_id': 'test', 'test' :'test'})

overwrite = False

print('------------------------------------------')
print('Creating knn dataset for each billboard id...')
print('------------------------------------------')

if len(sys.argv) == 2:
    if sys.argv[1] == 'overwrite':
        print('Overwrite Mode On')
        overwrite = True
else:
    print('Overwrite Mode Off')

client = boto3.client('athena')

threads = []
output_filename = 'result_knn.csv'

available_dates = ['20191101',]

header_one = ['audience_segment_id', 'max']
header_two = ['billboard_id', 'audience_segment_id', 'count', 'year', 'quarter', 'month', 'week_of_year']
header_three = ['billboard_id', 'audience_id_one', 'audience_id_two', 'audience_id_three', 'count']
alphabet = ['a', 'b', 'c', 'd', 'e', 'f']


queries = ["""
SELECT billboard_id, v['1'] AS a1,v['2'] AS a2,v['3'] AS a3,v['4'] AS a4,v['5'] AS a5,v['6'] AS a6,v['7'] AS a7,v['8'] AS a8,v['9'] AS a9,v['10'] AS a10,v['11'] AS a11,v['12'] AS a12,v['13'] AS a13,v['14'] AS a14,v['15'] AS a15,v['16'] AS a16,v['17'] AS a17,v['18'] AS a18,v['19'] AS a19,v['20'] AS a20,v['21'] AS a21,v
['22'] AS a22,v['23'] AS a23,v['24'] AS a24,v['25'] AS a25,v['26'] AS a26,v
['27'] AS a27,v['28'] AS a28,v['29'] AS a29,v['30'] AS a30,v['31'] AS a31,v
['32'] AS a32,v['33'] AS a33,v['34'] AS a34,v['35'] AS a35,v['36'] AS a36,v
['37'] AS a37,v['38'] AS a38,v['39'] AS a39,v['40'] AS a40,v['41'] AS a41,v
['42'] AS a42,v['43'] AS a43,v['44'] AS a44,v['45'] AS a45,v['46'] AS a46,v
['47'] AS a47,v['48'] AS a48,v['49'] AS a49,v['50'] AS a50,v['51'] AS a51,v
['52'] AS a52,v['53'] AS a53,v['54'] AS a54,v['55'] AS a55,v['56'] AS a56,v
['57'] AS a57,v['58'] AS a58,v['59'] AS a59,v['60'] AS a60,v['61'] AS a61,v
['62'] AS a62,v['63'] AS a63,v['64'] AS a64,v['65'] AS a65,v['66'] AS a66,v
['67'] AS a67,v['68'] AS a68,v['69'] AS a69,v['70'] AS a70,v['71'] AS a71,v
['72'] AS a72,v['73'] AS a73,v['74'] AS a74,v['75'] AS a75,v['76'] AS a76,v
['77'] AS a77,v['78'] AS a78,v['79'] AS a79,v['80'] AS a80,v['81'] AS a81,v
['82'] AS a82,v['83'] AS a83,v['84'] AS a84,v['85'] AS a85,v['86'] AS a86,v
['87'] AS a87,v['88'] AS a88,v['89'] AS a89,v['90'] AS a90,v['91'] AS a91,v
['92'] AS a92,v['93'] AS a93,v['94'] AS a94,v['95'] AS a95,v['96'] AS a96,v
['97'] AS a97,v['98'] AS a98,v['99'] AS a99,v['100'] AS a100
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '""",

"""
SELECT billboard_id, v['101'] AS a101,v['102'] AS a102,v['103'] AS a103,v['104'] AS a104,v['105'] AS a105,v['106'] AS a106,v['107'] AS a107,v['108'] AS a108,v['109'] AS a109,v['110'] AS a110,v['111'] AS a111,v['112'] AS a112,v['113'] AS a113,v['114'] AS a114,v['115'] AS a115,v['116'] AS a116,v['117'] AS a117,v['118'] AS a118,v['119'] AS a119,v['120'] AS a120,v['121'] AS a121,v['122'] AS a122,v['123'] AS a123,v['124'] AS a124,v['125'] AS a125,v['126'] AS a126,v['127'] AS a127,v['128']AS a128,v['129'] AS a129,v['130'] AS a130,v['131'] AS a131,v['132'] AS a132,v['133'] AS a133,v['134'] AS a134,v['135'] AS a135,v['136'] AS a136,v['137'] AS a137,v['138'] AS a138,v['139'] AS a139,v['140'] AS a140,v['141'] AS a141,v['142'] AS a142,v['143'] AS a143,v['144'] AS a144,v['145'] AS a145,v['146'] AS a146,v['147'] AS a147,v['148'] AS a148,v['149'] AS a149,v['150'] AS a150,v['151'] AS a151,v['152'] AS a152,v['153'] AS a153,v['154'] AS a154,v['155'] AS a155,v['156'] AS a156,v['157'] AS a157,v['158'] AS a158,v['159'] ASa159,v['160'] AS a160,v['161'] AS a161,v['162'] AS a162,v['163'] AS a163,v['164'] AS a164,v['165'] AS a165,v['166'] AS a166,v['167'] AS a167,v['168']AS a168,v['169'] AS a169,v['170'] AS a170,v['171'] AS a171,v['172'] AS a172,v['173'] AS a173,v['174'] AS a174,v['175'] AS a175,v['176'] AS a176,v['177'] AS a177,v['178'] AS a178,v['179'] AS a179,v['180'] AS a180,v['181'] AS a181,v['182'] AS a182,v['183'] AS a183,v['184'] AS a184,v['185'] AS a185,v['186'] AS a186,v['187'] AS a187,v['188'] AS a188,v['189'] AS a189,v['190'] AS a190,v['191'] AS a191,v['192'] AS a192,v['193'] AS a193,v['194'] AS a194,v['195'] AS a195,v['196'] AS a196,v['197'] AS a197,v['198'] AS a198,v['199'] ASa199,v['200'] AS a200
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['201'] AS a201,v['202'] AS a202,v['203'] AS a203,v['204'] AS a204,v['205'] AS a205,v['206'] AS a206,v['207'] AS a207,v['208']AS a208,v['209'] AS a209,v['210'] AS a210,v['211'] AS a211,v['212'] AS a212,v['213'] AS a213,v['214'] AS a214,v['215'] AS a215,v['216'] AS a216,v['217'] AS a217,v['218'] AS a218,v['219'] AS a219,v['220'] AS a220,v['221'] AS a221,v['222'] AS a222,v['223'] AS a223,v['224'] AS a224,v['225'] AS a225,v['226'] AS a226,v['227'] AS a227,v['228'] AS a228,v['229'] AS a229,v['230'] AS a230,v['231'] AS a231,v['232'] AS a232,v['233'] AS a233,v['234'] AS a234,v['235'] AS a235,v['236'] AS a236,v['237'] AS a237,v['238'] AS a238,v['239'] AS a239,v['240'] AS a240,v['241'] AS a241,v['242'] AS a242,v['243'] AS a243,v['244'] AS a244,v['245'] AS a245,v['246'] AS a246,v['247'] AS a247,v['248']AS a248,v['249'] AS a249,v['250'] AS a250,v['251'] AS a251,v['252'] AS a252,v['253'] AS a253,v['254'] AS a254,v['255'] AS a255,v['256'] AS a256,v['257'] AS a257,v['258'] AS a258,v['259'] AS a259,v['260'] AS a260,v['261'] AS a261,v['262'] AS a262,v['263'] AS a263,v['264'] AS a264,v['265'] AS a265,v['266'] AS a266,v['267'] AS a267,v['268'] AS a268,v['269'] AS a269,v['270'] AS a270,v['271'] AS a271,v['272'] AS a272,v['273'] AS a273,v['274'] AS a274,v['275'] AS a275,v['276'] AS a276,v['277'] AS a277,v['278'] AS a278,v['279'] AS a279,v['280'] AS a280,v['281'] AS a281,v['282'] AS a282,v['283'] AS a283,v['284'] AS a284,v['285'] AS a285,v['286'] AS a286,v['287'] AS a287,v['288']AS a288,v['289'] AS a289,v['290'] AS a290,v['291'] AS a291,v['292'] AS a292,v['293'] AS a293,v['294'] AS a294,v['295'] AS a295,v['296'] AS a296,v['297'] AS a297,v['298'] AS a298,v['299'] AS a299,v['300'] AS a300
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['301'] AS a301,v['302'] AS a302,v['303'] AS a303,v['304'] AS a304,v['305'] AS a305,v['306'] AS a306,v['307'] AS a307,v['308'] AS a308,v['309'] AS a309,v['310'] AS a310,v['311'] AS a311,v['312'] AS a312,v['313'] AS a313,v['314'] AS a314,v['315'] AS a315,v['316'] AS a316,v['317'] AS a317,v['318'] AS a318,v['319'] AS a319,v['320'] AS a320,v['321'] AS a321,v['322'] AS a322,v['323'] AS a323,v['324'] AS a324,v['325'] AS a325,v['326'] AS a326,v['327'] AS a327,v['328']AS a328,v['329'] AS a329,v['330'] AS a330,v['331'] AS a331,v['332'] AS a332,v['333'] AS a333,v['334'] AS a334,v['335'] AS a335,v['336'] AS a336,v['337'] AS a337,v['338'] AS a338,v['339'] AS a339,v['340'] AS a340,v['341'] AS a341,v['342'] AS a342,v['343'] AS a343,v['344'] AS a344,v['345'] AS a345,v['346'] AS a346,v['347'] AS a347,v['348'] AS a348,v['349'] AS a349,v['350'] AS a350,v['351'] AS a351,v['352'] AS a352,v['353'] AS a353,v['354'] AS a354,v['355'] AS a355,v['356'] AS a356,v['357'] AS a357,v['358'] AS a358,v['359'] AS a359,v['360'] AS a360,v['361'] AS a361,v['362'] AS a362,v['363'] AS a363,v['364'] AS a364,v['365'] AS a365,v['366'] AS a366,v['367'] AS a367,v['368']AS a368,v['369'] AS a369,v['370'] AS a370,v['371'] AS a371,v['372'] AS a372,v['373'] AS a373,v['374'] AS a374,v['375'] AS a375,v['376'] AS a376,v['377'] AS a377,v['378'] AS a378,v['379'] AS a379,v['380'] AS a380,v['381'] AS a381,v['382'] AS a382,v['383'] AS a383,v['384'] AS a384,v['385'] AS a385,v['386'] AS a386,v['387'] AS a387,v['388'] AS a388,v['389'] AS a389,v['390'] AS a390,v['391'] AS a391,v['392'] AS a392,v['393'] AS a393,v['394'] AS a394,v['395'] AS a395,v['396'] AS a396,v['397'] AS a397,v['398'] AS a398,v['399'] AS a399,v['400'] AS a400
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['401'] AS a401,v['402'] AS a402,v['403'] AS a403,v['404'] AS a404,v['405'] AS a405,v['406'] AS a406,v['407'] AS a407,v['408']AS a408,v['409'] AS a409,v['410'] AS a410,v['411'] AS a411,v['412'] AS a412,v['413'] AS a413,v['414'] AS a414,v['415'] AS a415,v['416'] AS a416,v['417'] AS a417,v['418'] AS a418,v['419'] AS a419,v['420'] AS a420,v['421'] AS a421,v['422'] AS a422,v['423'] AS a423,v['424'] AS a424,v['425'] AS a425,v['426'] AS a426,v['427'] AS a427,v['428'] AS a428,v['429'] AS a429,v['430'] AS a430,v['431'] AS a431,v['432'] AS a432,v['433'] AS a433,v['434'] AS a434,v['435'] AS a435,v['436'] AS a436,v['437'] AS a437,v['438'] AS a438,v['439'] AS a439,v['440'] AS a440,v['441'] AS a441,v['442'] AS a442,v['443'] AS a443,v['444'] AS a444,v['445'] AS a445,v['446'] AS a446,v['447'] AS a447,v['448']AS a448,v['449'] AS a449,v['450'] AS a450,v['451'] AS a451,v['452'] AS a452,v['453'] AS a453,v['454'] AS a454,v['455'] AS a455,v['456'] AS a456,v['457'] AS a457,v['458'] AS a458,v['459'] AS a459,v['460'] AS a460,v['461'] AS a461,v['462'] AS a462,v['463'] AS a463,v['464'] AS a464,v['465'] AS a465,v['466'] AS a466,v['467'] AS a467,v['468'] AS a468,v['469'] AS a469,v['470'] AS a470,v['471'] AS a471,v['472'] AS a472,v['473'] AS a473,v['474'] AS a474,v['475'] AS a475,v['476'] AS a476,v['477'] AS a477,v['478'] AS a478,v['479'] AS a479,v['480'] AS a480,v['481'] AS a481,v['482'] AS a482,v['483'] AS a483,v['484'] AS a484,v['485'] AS a485,v['486'] AS a486,v['487'] AS a487,v['488']AS a488,v['489'] AS a489,v['490'] AS a490,v['491'] AS a491,v['492'] AS a492,v['493'] AS a493,v['494'] AS a494,v['495'] AS a495,v['496'] AS a496,v['497'] AS a497,v['498'] AS a498,v['499'] AS a499,v['500'] AS a500
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['501'] AS a501,v['502'] AS a502,v['503'] AS a503,v['504'] AS a504,v['505'] AS a505,v['506'] AS a506,v['507'] AS a507,v['508'] AS a508,v['509'] AS a509,v['510'] AS a510,v['511'] AS a511,v['512'] AS a512,v['513'] AS a513,v['514'] AS a514,v['515'] AS a515,v['516'] AS a516,v['517'] AS a517,v['518'] AS a518,v['519'] AS a519,v['520'] AS a520,v['521'] AS a521,v['522'] AS a522,v['523'] AS a523,v['524'] AS a524,v['525'] AS a525,v['526'] AS a526,v['527'] AS a527,v['528']AS a528,v['529'] AS a529,v['530'] AS a530,v['531'] AS a531,v['532'] AS a532,v['533'] AS a533,v['534'] AS a534,v['535'] AS a535,v['536'] AS a536,v['537'] AS a537,v['538'] AS a538,v['539'] AS a539,v['540'] AS a540,v['541'] AS a541,v['542'] AS a542,v['543'] AS a543,v['544'] AS a544,v['545'] AS a545,v['546'] AS a546,v['547'] AS a547,v['548'] AS a548,v['549'] AS a549,v['550'] AS a550,v['551'] AS a551,v['552'] AS a552,v['553'] AS a553,v['554'] AS a554,v['555'] AS a555,v['556'] AS a556,v['557'] AS a557,v['558'] AS a558,v['559'] AS a559,v['560'] AS a560,v['561'] AS a561,v['562'] AS a562,v['563'] AS a563,v['564'] AS a564,v['565'] AS a565,v['566'] AS a566,v['567'] AS a567,v['568']AS a568,v['569'] AS a569,v['570'] AS a570,v['571'] AS a571,v['572'] AS a572,v['573'] AS a573,v['574'] AS a574,v['575'] AS a575,v['576'] AS a576,v['577'] AS a577,v['578'] AS a578,v['579'] AS a579,v['580'] AS a580,v['581'] AS a581,v['582'] AS a582,v['583'] AS a583,v['584'] AS a584,v['585'] AS a585,v['586'] AS a586,v['587'] AS a587,v['588'] AS a588,v['589'] AS a589,v['590'] AS a590,v['591'] AS a591,v['592'] AS a592,v['593'] AS a593,v['594'] AS a594,v['595'] AS a595,v['596'] AS a596,v['597'] AS a597,v['598'] AS a598,v['599'] AS a599,v['600'] AS a600
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['601'] AS a601,v['602'] AS a602,v['603'] AS a603,v['604'] AS a604,v['605'] AS a605,v['606'] AS a606,v['607'] AS a607,v['608']AS a608,v['609'] AS a609,v['610'] AS a610,v['611'] AS a611,v['612'] AS a612,v['613'] AS a613,v['614'] AS a614,v['615'] AS a615,v['616'] AS a616,v['617'] AS a617,v['618'] AS a618,v['619'] AS a619,v['620'] AS a620,v['621'] AS a621,v['622'] AS a622,v['623'] AS a623,v['624'] AS a624,v['625'] AS a625,v['626'] AS a626,v['627'] AS a627,v['628'] AS a628,v['629'] AS a629,v['630'] AS a630,v['631'] AS a631,v['632'] AS a632,v['633'] AS a633,v['634'] AS a634,v['635'] AS a635,v['636'] AS a636,v['637'] AS a637,v['638'] AS a638,v['639'] AS a639,v['640'] AS a640,v['641'] AS a641,v['642'] AS a642,v['643'] AS a643,v['644'] AS a644,v['645'] AS a645,v['646'] AS a646,v['647'] AS a647,v['648']AS a648,v['649'] AS a649,v['650'] AS a650,v['651'] AS a651,v['652'] AS a652,v['653'] AS a653,v['654'] AS a654,v['655'] AS a655,v['656'] AS a656,v['657'] AS a657,v['658'] AS a658,v['659'] AS a659,v['660'] AS a660,v['661'] AS a661,v['662'] AS a662,v['663'] AS a663,v['664'] AS a664,v['665'] AS a665,v['666'] AS a666,v['667'] AS a667,v['668'] AS a668,v['669'] AS a669,v['670'] AS a670,v['671'] AS a671,v['672'] AS a672,v['673'] AS a673,v['674'] AS a674,v['675'] AS a675,v['676'] AS a676,v['677'] AS a677,v['678'] AS a678,v['679'] AS a679,v['680'] AS a680,v['681'] AS a681,v['682'] AS a682,v['683'] AS a683,v['684'] AS a684,v['685'] AS a685,v['686'] AS a686,v['687'] AS a687,v['688']AS a688,v['689'] AS a689,v['690'] AS a690,v['691'] AS a691,v['692'] AS a692,v['693'] AS a693,v['694'] AS a694,v['695'] AS a695,v['696'] AS a696,v['697'] AS a697,v['698'] AS a698,v['699'] AS a699,v['700'] AS a700
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['701'] AS a701,v['702'] AS a702,v['703'] AS a703,v['704'] AS a704,v['705'] AS a705,v['706'] AS a706,v['707'] AS a707,v['708'] AS a708,v['709'] AS a709,v['710'] AS a710,v['711'] AS a711,v['712'] AS a712,v['713'] AS a713,v['714'] AS a714,v['715'] AS a715,v['716'] AS a716,v['717'] AS a717,v['718'] AS a718,v['719'] AS a719,v['720'] AS a720,v['721'] AS a721,v['722'] AS a722,v['723'] AS a723,v['724'] AS a724,v['725'] AS a725,v['726'] AS a726,v['727'] AS a727,v['728']AS a728,v['729'] AS a729,v['730'] AS a730,v['731'] AS a731,v['732'] AS a732,v['733'] AS a733,v['734'] AS a734,v['735'] AS a735,v['736'] AS a736,v['737'] AS a737,v['738'] AS a738,v['739'] AS a739,v['740'] AS a740,v['741'] AS a741,v['742'] AS a742,v['743'] AS a743,v['744'] AS a744,v['745'] AS a745,v['746'] AS a746,v['747'] AS a747,v['748'] AS a748,v['749'] AS a749,v['750'] AS a750,v['751'] AS a751,v['752'] AS a752,v['753'] AS a753,v['754'] AS a754,v['755'] AS a755,v['756'] AS a756,v['757'] AS a757,v['758'] AS a758,v['759'] AS a759,v['760'] AS a760,v['761'] AS a761,v['762'] AS a762,v['763'] AS a763,v['764'] AS a764,v['765'] AS a765,v['766'] AS a766,v['767'] AS a767,v['768']AS a768,v['769'] AS a769,v['770'] AS a770,v['771'] AS a771,v['772'] AS a772,v['773'] AS a773,v['774'] AS a774,v['775'] AS a775,v['776'] AS a776,v['777'] AS a777,v['778'] AS a778,v['779'] AS a779,v['780'] AS a780,v['781'] AS a781,v['782'] AS a782,v['783'] AS a783,v['784'] AS a784,v['785'] AS a785,v['786'] AS a786,v['787'] AS a787,v['788'] AS a788,v['789'] AS a789,v['790'] AS a790,v['791'] AS a791,v['792'] AS a792,v['793'] AS a793,v['794'] AS a794,v['795'] AS a795,v['796'] AS a796,v['797'] AS a797,v['798'] AS a798,v['799'] AS a799,v['800'] AS a800
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""  ,

"""
SELECT billboard_id, v['801'] AS a801,v['802'] AS a802,v['803'] AS a803,v['804'] AS a804,v['805'] AS a805,v['806'] AS a806,v['807'] AS a807,v['808']AS a808,v['809'] AS a809,v['810'] AS a810,v['811'] AS a811,v['812'] AS a812,v['813'] AS a813,v['814'] AS a814,v['815'] AS a815,v['816'] AS a816,v['817'] AS a817,v['818'] AS a818,v['819'] AS a819,v['820'] AS a820,v['821'] AS a821,v['822'] AS a822,v['823'] AS a823,v['824'] AS a824,v['825'] AS a825,v['826'] AS a826,v['827'] AS a827,v['828'] AS a828,v['829'] AS a829,v['830'] AS a830,v['831'] AS a831
FROM (
SELECT billboard_id, map_agg(audience_segment_id, count) v
FROM
(SELECT a.billboard_id as billboard_id, c.id as audience_segment_id, count(distinct a.mobile_device_id) as count FROM
(SELECT * FROM location_data.billboard_devices_partitioned WHERE dt=20191101 and billboard_id like '"""

]
def runKNN(num, i):
    output_path = 'knn_' + num + '_part_' + str(i) + '.csv'
    if os.path.exists(output_path) and not overwrite:
        print(output_path + ' already saved')
    else:
        print("Querying... for " + output_path)
        query = queries[i] + num + """%') a LEFT JOIN
        (SELECT * FROM location_data.device_audiences_partitioned WHERE dt=20191101) b ON a.mobile_device_id = b.mobile_device_id LEFT JOIN
        location_data.adomni_audience_segment c ON b.audience = c.placeiqid
        GROUP BY a.billboard_id, c.id) abc
        GROUP BY billboard_id
        ) d"""
        response = client.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {'Database': 'default'},
            ResultConfiguration = {
                'OutputLocation': 's3://athena-output-usf',
                'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
                }
            }
        )
        id = response['QueryExecutionId']

        while (client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State'] == 'RUNNING'):
            #print("Waiting... "  + num)
            sleep(5)
        status = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State']
        reason = ""
        if 'StateChangeReason' in client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']:
            reason = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
        status_query = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Query']
        outputLocation = client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['ResultConfiguration']['OutputLocation']

        if status is not "FAILED":
            os.system("aws s3 cp " + outputLocation + " .")
            os.system("rm " + output_path)
            os.system("mv " + id + ".csv " + output_path)
        #print(status_query)
        print("KNN for " + num)
        print(status)
        print(reason)
        #print(outputLocation)

start_time = time.time()

for a in itertools.chain(range (0, 10), alphabet):
    for i in range(0,8):
        t3 = Thread(target=runKNN, args=(str(a), i))
        threads.append(t3)
        t3.start()
    for x in threads:
        x.join()
        threads.remove(x)

result_df = pd.DataFrame()


#for hq

for i in itertools.chain(range (0, 10), alphabet):
    combined_df = pd.DataFrame()
    for a in range(0,8):
        output_path = 'knn_' + str(i) + '_part_' + str(a) + '.csv'
        temp_df = pd.read_csv(output_path)
        print(output_path + ' read')
        if a == 0:
            combined_df = temp_df
        else:
            combined_df = pd.merge(combined_df, temp_df, how='left', on=['billboard_id'])
    result_df = result_df.merge(combined_df, how='outer')

    #os.system("rm max_" + d + "_" + str(i) + ".csv")

billboard_info_path = 'billboards_20191112.csv'
billboard_info_df = pd.read_csv(billboard_info_path)
billboard_info_df.rename(columns={'locationHash':'billboard_id'})

final_df = pd.merge(billboard_info_df, result_df, how='left', on=['billboard_id'])
    #os.system("aws s3 cp " + temp_filename + " s3://result-output/high_quality/")

elapsed_time = time.time() - start_time
print('Finished. Elapsed Time: ' + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
#
# #print(result_df.head())
final_df.to_csv(output_filename, encoding='utf-8', index=False)
# os.system("aws s3 cp " + output_filename + " s3://result-output/")
