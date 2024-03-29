#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"


#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);//curent index size
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */

GenericKey *InternalPage::KeyAt(int index) {
    return array_[index].first;
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
    array_[index].first = key;
}

page_id_t InternalPage::ValueAt(int index) const {
    return array_[index].second;
}

void InternalPage::SetValueAt(int index, page_id_t value) {
    array_[index].second = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    int start = 1;
    int end = GetSize() - 1;
    while (start <= end)
    {
        int middle = (start + end) / 2;
        if (KM.CompareKeys(array_[middle].first, key) <= 0)  //middle.first
        {
            start = middle + 1;
        }
        else {
            end = middle - 1;
        }
    }
    return array_[start - 1].second;
    //return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    array_[0].second = old_value;
    array_[1].first = new_key;
    array_[1].second = new_value;
    SetSize(2);  //SetSize(2);
    
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  //return 0;
    int temp_index = ValueIndex(old_value) + 1;   //find the old index' right space
    for (int i = GetSize(); i > temp_index; i--) {    // make the right space empty, laters move to right one space
        array_[i].first = array_[i - 1].first;
        array_[i].second = array_[i - 1].second;
    }
    array_[temp_index].first = new_key;   //insert 
    array_[temp_index].second = new_value;
    IncreaseSize(1);    //renew the size
    return GetSize();   //return new size
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    // assert(size == GetMaxSize() + 1);
    int start = GetMaxSize() / 2;
    int length = size - start;
    recipient->CopyNFrom(array_ + GetMaxSize() / 2, length, buffer_pool_manager);
    SetSize(GetMinSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    std::copy((MappingType*)src, (MappingType*)src + size, array_ + GetSize());
    for (int i = GetSize(); i < GetSize() + size; i++) {
        Page* child_page = buffer_pool_manager->FetchPage(ValueAt(i));
        BPlusTreePage* child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
        child_node->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    int size = GetSize();
    if (index >= 0 && index < size) {
        for (int i = index + 1; i < size; i++) array_[i - 1] = array_[i];
        IncreaseSize(-1);
    }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() { 
    SetSize(0);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    // assert(GetSize() + recipient->GetSize() <= GetMaxSize());
    SetKeyAt(0, middle_key);
    recipient->CopyNFrom(array_, size, buffer_pool_manager);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(0, middle_key);
    recipient->CopyLastFrom(array_[0].first,array_[0].second, buffer_pool_manager);
    Remove(0);
    Page* parent = buffer_pool_manager->FetchPage(GetParentPageId());
    InternalPage* p = reinterpret_cast<InternalPage*>(parent);
    int x = p->ValueIndex(GetPageId());
    p->SetKeyAt(x, KeyAt(0));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    array_[size].first = key;
    array_[size].second = value;
    Page* page = buffer_pool_manager->FetchPage(ValueAt(size));
    BPlusTreePage* datapage = reinterpret_cast<BPlusTreePage*>(page->GetData());
    datapage->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyFirstFrom(array_[size - 1].second, buffer_pool_manager);
    IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    for (int i = size; i > 0; i--) {
        array_[i] = array_[i - 1];
    }
    array_[0].second = value;
    Page* page = buffer_pool_manager->FetchPage(ValueAt(0));
    BPlusTreePage* datapage = reinterpret_cast<BPlusTreePage*>(page->GetData());
    datapage->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    Page* parent = buffer_pool_manager->FetchPage(GetParentPageId());
    InternalPage* p = reinterpret_cast<InternalPage*>(parent);
    int x = p->ValueIndex(GetPageId());
    p->SetKeyAt(x, KeyAt(0));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
    IncreaseSize(1);
}