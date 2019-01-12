from src.riko import RikoConfig, DictModel, ObjectModel


class BlogArticle(ObjectModel):
    ak = "aid"
    pk = ["aid"]

    def __init__(self):
        super().__init__()
        self.aid = None
        self.author_uid = ""
        self.title = ""
        self.content = ""


class BlogUser(DictModel):
    ak = "uid"
    pk = ["uid"]
    fields = ["username", "age"]


if __name__ == '__main__':
    RikoConfig.update_default(database="blog")
    pass
    refind_ba = (BlogArticle
                 .select(_columns=('title', ))
                 .distinct()
                 .where(author_uid=12)
                 .where_raw("aid <= %(aid_limit)s")
                 .get({'aid_limit': 3}))
    pass
    ba = BlogArticle.create(author_uid=12, title="Koito yuu", content="Koito yuu love Nanami Touko.")
    ba.insert()
    pass
    ba.content += "啊哈？"
    ba.save()
    pass
    pass
    pass
    user_link = (BlogUser
                 .select()
                 .where(username="Rinka")
                 .limit(5)
                 .offset(1)
                 .order_by("age")
                 .get())
    pass
    user_cols = BlogUser.get(_columns=("username", "age"), age=23)
    pass
    user1 = BlogUser.create(username="Rinka", age=25)
    uid = user1.insert()
    pass
    with user1.db_session_.transaction():
        user1["username"] = "Homura"
        user1.save()
        user_pk_conflict = BlogUser()
        user_pk_conflict["uid"] = 12
        user_pk_conflict["username"] = "Test"
        user_pk_conflict["age"] = 168
        user_pk_conflict.insert()
        t = 1 / 0
        user1["age"] = 43
        user1.save()
    pass
    user_research_1 = BlogUser.get_one(uid=uid)
    pass
    user_all = BlogUser.get()
    pass
    user1.delete()
    pass

