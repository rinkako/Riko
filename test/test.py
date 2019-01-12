from src.riko import RikoConfig, DictModel


class BlogArticle(DictModel):
    ak = "aid"
    pk = ["aid"]
    fields = ["author_uid", "titile", "content"]


class BlogUser(DictModel):
    ak = "uid"
    pk = ["uid"]
    fields = ["username", "age"]


if __name__ == '__main__':
    RikoConfig.update_default(database="blog")
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
    user1 = BlogUser()
    user1["username"] = "Rinka"
    user1["age"] = 25
    uid = user1.insert()
    pass
    user1["age"] = 43
    user1.save()
    pass
    user_research_1 = BlogUser.get_one(uid=uid)
    pass
    user_all = BlogUser.get()
    pass
    user1.delete()
    pass
