from src.riko import Riko, DictModel, ObjectModel, INSERT


class BlogArticle(ObjectModel):
    """
    Model object defined in `object` like class.
    """
    ak = "aid"  # ak (auto increment id) name declaration, the field will be update when call `insert()`
    pk = ["aid"]  # primary keys name declaration, for identified a object for performing ORM operations

    def __init__(self):
        super().__init__()
        self.aid = None
        self.author_uid = ""
        self.title = ""
        self.content = ""


class BlogRating(ObjectModel):
    """
    Model object defined in `object` like class.
    """
    ak = "aid"
    pk = ["aid"]

    def __init__(self):
        super().__init__()
        self.aid = None
        self.rating = 0


class BlogUser(DictModel):
    """
    Model object defined in `dict` like class.
    """
    ak = "uid"
    pk = ["uid"]
    fields = ["username", "age"]


if __name__ == '__main__':
    # set db config
    Riko.update_default(database="blog")

    # create object
    article1 = BlogArticle.create(author_uid=12, title="Koito yuu", content="Koito yuu love Nanami Touko.")
    # return auto increment id, the object `aid` will be set to this id automatically since declared at `ak` meta
    article1_id = article1.insert()
    # update object fields
    article1.content += " (updated)"
    article1.save()
    # delete object
    article1.delete()

    # get all article
    all_article = BlogArticle.get()
    # get some column with condition
    article_list1 = BlogArticle.get(return_columns=("title",), _where_raw=("aid < 10",))
    article_list2 = BlogArticle.get(return_columns=("title", "content"), aid=1, author_uid=12)
    # order, limit and offset
    article_page1 = BlogArticle.get(return_columns=("title",), _order="title", _limit=5, _offset=1)
    article_page2 = BlogArticle.get(return_columns=("title",), _order=("title", "author_uid"), _limit=5, _offset=1)

    # select query
    select_result1 = (BlogUser
                      .select()
                      .where(username="Rinka")
                      .pagination(1, 3)
                      .order_by("age")
                      .get())
    select_result2 = (BlogArticle
                      .select(return_columns=('title',))
                      .alias("t")
                      .distinct()
                      .where(author_uid=12)
                      .where_raw("t.aid <= %(aid_limit)s")
                      .get({'aid_limit': 3}))

    # insert query
    insert_id = (BlogRating
                 .insert_query()
                 .values(aid=233, rating=99)
                 .go(return_last_id=True))

    # batch insert
    articles2insert = list()
    articles2insert.append((12, "Bloom into you 1", "Test content 1"))
    articles2insert.append((12, "Bloom into you 2", "Test content 2"))
    articles2insert.append((12, "Bloom into you 3", "Test content 3"))
    affected_row1 = (BlogArticle
                     .insert_many()
                     .values(["author_uid", "title", "content"], articles2insert)
                     .go())
    article_x4 = BlogArticle.create(author_uid=13, title="Bloom into you 4", content="Test content 4")
    article_x5 = BlogArticle.create(author_uid=13, title="Bloom into you 5", content="Test content 5")
    affected_row2 = (BlogArticle
                     .insert_many()
                     .from_objects([article_x4, article_x5])
                     .go())

    # delete query
    affected_row3 = (BlogRating
                     .delete_query()
                     .where(rating=99)
                     .go())
    affected_row4 = (BlogRating
                     .delete_query()
                     .where_raw("aid >= 6", "aid <= 7")
                     .go())
    # BlogRating.delete_query().go()  # delete all

    # left join table
    left = (BlogArticle
            .select()
            .alias("a")
            .left_join(BlogRating, alias="r", on=("a.aid = r.aid",))
            .get())
    # right join table
    right = (BlogArticle
             .select()
             .alias("a")
             .right_join(BlogUser, alias="u", on="u.uid = a.author_uid")
             .get())
    # natural join table
    rating_info = (BlogArticle
                   .select()
                   .natural_join(BlogRating)
                   .get())
    # inner join table
    article_author = (BlogArticle
                      .select(return_columns=('u.username', 'a.title'))
                      .alias("a")
                      .join(BlogUser, on=("a.author_uid = u.uid",), alias="u")
                      .order_by("username")
                      .get())

    # insert with primary key conflict
    user_pk_conflict = BlogUser.create(uid=1, username="Test_Dupicate", age=168)
    try:
        user_pk_conflict.insert()
    except:
        print("conflict!")
    user_pk_conflict.insert(on_duplicate_key_replace=INSERT.DUPLICATE_KEY_IGNORE)
    user_pk_conflict.insert(on_duplicate_key_replace=INSERT.DUPLICATE_KEY_REPLACE)
    # on duplicate key update
    user_pk_conflict.insert(on_duplicate_key_replace=INSERT.DUPLICATE_KEY_UPDATE, age=user_pk_conflict["age"] + 1)

    # count with condition
    article_number1 = BlogArticle.count(aid=3)
    article_number2 = BlogArticle.count(_where_raw=("aid <= 3",))

    # existence
    article_existence = BlogArticle.has(_where_raw=("aid <= 3",))
    existence_no = BlogArticle.has(aid=-1)
    existence_yes = BlogArticle.has(aid=3, title="Koito yuu")

    # transaction
    article_tx = BlogArticle.create(author_uid=15, title="Transaction test", content="Aha, a transaction.")
    article_tx.insert()
    with article_tx.db_session_.transaction():
        article_tx.title = "Transaction test (title updated)"
        article_tx.save()
        # t = 1 / 0  # uncomment this to raise exception, and transaction will rollback
        article_tx.content = "Aha, a transaction. (content updated)"
        article_tx.save()
