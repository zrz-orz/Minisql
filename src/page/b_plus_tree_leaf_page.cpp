#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetSize(0);
    SetNextPageId(INVALID_PAGE_ID);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    int st = 0, ed = GetSize() - 1;
    while (st <= ed) {  // find the last key in array <= input
        int mid = (ed - st) / 2 + st;
        if (KM.CompareKeys(array_[mid].first, key) >= 0)
            ed = mid - 1;
        else
            st = mid + 1;
    }
    return ed + 1;
    //return 0;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
    return array_[index].first;
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
    array_[index].first = key;
}

RowId LeafPage::ValueAt(int index) const {
    return array_[index].second;
}

void LeafPage::SetValueAt(int index, RowId value) {
  array_[index].second = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    int size = GetSize();
    int now_key = KeyIndex(key,KM);
    for (int i = size; i > now_key; i--) {
        array_[i].first = array_[i - 1].first;
        array_[i].second = array_[i - 1].second;
    }
    array_[now_key].first = key;
    array_[now_key].second = value;
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
    int size = GetSize();

    int start = GetMaxSize() / 2;
    int length = size - start;
    recipient->CopyNFrom(array_ + start, length);
    SetSize(start);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
    std::copy((MapType*)src, (MapType*)src + size, array_ + GetSize());
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    int size = GetSize();
    int now_key = KeyIndex(key, KM);
    if (now_key < size && KM.CompareKeys(key, KeyAt(now_key)) == 0) {
        value = array_[now_key].second;
        return true;
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    int size = GetSize();
    int now_key = KeyIndex(key, KM);
    if (now_key < size && KM.CompareKeys(key, KeyAt(now_key)) == 0) {
        for (int i = now_key; i < size - 1; i++) array_[i] = array_[i + 1];
        IncreaseSize(-1);
        return GetSize();
    }
    else
        return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
    assert(recipient != nullptr);

    int recipient_start = recipient->GetSize();
    for (int i = 0; i < GetSize(); i++) {
        recipient->array_[recipient_start + i].first = array_[i].first;
        recipient->array_[recipient_start + i].second = array_[i].second;
    }
    recipient->SetNextPageId(GetNextPageId());  //update the next_page id
    recipient->IncreaseSize(GetSize());
    SetNextPageId(INVALID_PAGE_ID);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    MapType pair = GetItem(0);    // the first item
    for (int i = 0; i < GetSize() - 1; i++) {
        array_[i].first = array_[i + 1].first;    //move the later item
        array_[i].second = array_[i + 1].second;
    }
    IncreaseSize(-1);
    recipient->CopyLastFrom(pair.first, pair.second);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
    int temp_size = this->GetSize();
    array_[temp_size].first = key;
    array_[temp_size].second = value;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    int temp_end_index = GetSize() - 1;
    MapType pair = GetItem(temp_end_index);
    IncreaseSize(-1);
    recipient->CopyFirstFrom(pair.first, pair.second);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
    IncreaseSize(1);
    for (int i = GetSize() - 1; i > 0; i--) {   //move one space 
        array_[i] = array_[i - 1];
    }
    array_[0].first = key;
    array_[0].second = value;
}