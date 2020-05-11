from revalution import Reprice

rv = Reprice("2020-05-07")
#rv.secid_from_excel("data.xlsx")

#print(rv.secid)

rv.secid_from_list(['dfdfd', 'dfdfd'])

print(rv.secid)

