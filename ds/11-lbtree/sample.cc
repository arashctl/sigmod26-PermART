bool lbtree::insert(key_type key, void *ptr)
{
    // record the path from root to leaf
    // parray[level] is a node on the path
    // child ppos[level] of parray[level] == parray[level-1]
    //
    Pointer8B parray[32]; // 0 .. root_level will be used
    short ppos[32];       // 1 .. root_level will be used
    bool isfull[32];      // 0 .. root_level will be used

    unsigned char key_hash = hashcode1B(key);
    volatile long long sum;

    /* Part 1. get the positions to insert the key */
    {
        bnode *p;
        bleaf *lp;
        int i, t, m, b;
    #ifdef VAR_KEY
        int c;
    #endif

    Again2:
        // 1. RTM begin
        if (_xbegin() != _XBEGIN_STARTED)
        {
            // random backoff
            sum= 0;
            for (int i=(rdtsc() % 1024); i>0; i--) sum += i;
            goto Again2;
        }

        // 2. search nonleaf nodes
        p = tree_meta->tree_root;

        for (i = tree_meta->root_level; i > 0; i--)
        {
        #ifdef PREFETCH
            // prefetch the entire node
            NODE_PREF(p);
        #endif

            // if the lock bit is set, abort
            if (p->lock())
            {
                _xabort(3);
                goto Again2;
            }

            parray[i] = p;
            isfull[i] = (p->num() == NON_LEAF_KEY_NUM);

            // binary search to narrow down to at most 8 entries
            b = 1;
            t = p->num();
            while (b + 7 <= t)
            {
                m = (b + t) >> 1;
            #ifdef VAR_KEY
                c = vkcmp((char*)p->k(m), (char*)key);
                if (c > 0)
                    b = m + 1;
                else if (c < 0)
                    t = m - 1;
            #else
                if (key > p->k(m))
                    b = m + 1;
                else if (key < p->k(m))
                    t = m - 1;
            #endif
                else
                {
                    p = p->ch(m);
                    ppos[i] = m;
                    goto inner_done;
                }
            }

            // sequential search (which is slightly faster now)
            for (; b <= t; b++)
            #ifdef VAR_KEY
                if (vkcmp((char*)key, (char*)p->k(b)) > 0)
                    break;
            #else
                if (key < p->k(b))
                    break;
            #endif
            p = p->ch(b - 1);
            ppos[i] = b - 1;

        inner_done:;
        }

        // 3. search leaf node
        lp = (bleaf *)p;

    #ifdef PREFETCH
        // prefetch the entire node
        LEAF_PREF(lp);
    #endif
        // if the lock bit is set, abort
        if (lp->lock)
        {
            _xabort(4);
            goto Again2;
        }

        parray[0] = lp;

        // SIMD comparison
        // a. set every byte to key_hash in a 16B register
        __m128i key_16B = _mm_set1_epi8((char)key_hash);

        // b. load meta into another 16B register
        __m128i fgpt_16B = _mm_load_si128((const __m128i *)lp);

        // c. compare them
        __m128i cmp_res = _mm_cmpeq_epi8(key_16B, fgpt_16B);

        // d. generate a mask
        unsigned int mask = (unsigned int)
            _mm_movemask_epi8(cmp_res); // 1: same; 0: diff

        // remove the lower 2 bits then AND bitmap
        mask = (mask >> 2) & ((unsigned int)(lp->bitmap));

        // search every matching candidate
        while (mask)
        {
            int jj = bitScan(mask) - 1; // next candidate
        #ifdef VAR_KEY
            if (vkcmp((char*)lp->k(jj), (char*)key) == 0)
            { // found: do nothing, return
                _xend();
                return;
            }
        #else
            if (lp->k(jj) == key)
            { // found: do nothing, return
                _xend();
                return false;
            }
        #endif
            mask &= ~(0x1 << jj); // remove this bit
            /*  UBSan: implicit conversion from int -33 to unsigned int 
                changed the value to 4294967263 (32-bit, unsigned)      */
        } // end while

        // 4. set lock bits before exiting the RTM transaction
        lp->lock = 1;

        isfull[0] = lp->isFull();
        if (isfull[0])
        {
            for (i = 1; i <= tree_meta->root_level; i++)
            {
                p = parray[i];
                p->lock() = 1;
                if (!isfull[i])
                    break;
            }
        }

        // 5. RTM commit
        _xend();

    } // end of Part 1

    /* Part 2. leaf node */
    {
        bleaf *lp = parray[0];
        bleafMeta meta = *((bleafMeta *)lp);

        /* 1. leaf is not full */
        if (!isfull[0])
        {
            #if !defined(NVMPOOL_REAL) || !defined(UNLOCK_AFTER)
            meta.v.lock = 0; // clear lock in temp meta
            #endif

            // 1.1 get first empty slot
            uint16_t bitmap = meta.v.bitmap;
            int slot = bitScan(~bitmap) - 1;

            // 1.2 set leaf.entry[slot]= (k, v);
            // set fgpt, bitmap in meta
            lp->k(slot) = key;
            lp->ch(slot) = ptr;
            meta.v.fgpt[slot] = key_hash;
            bitmap |= (1 << slot);

            // 1.3 line 0: 0-2; line 1: 3-6; line 2: 7-10; line 3: 11-13
            // in line 0?
            if (slot < 3)
            {
                // 1.3.1 write word 0
                meta.v.bitmap = bitmap;

                #if defined(NVMPOOL_REAL) && defined(NONTEMP) 
                lp->setWord0_temporal(&meta);
                #else
                lp->setWord0(&meta);
                #endif
                // 1.3.2 flush
                #ifdef NVMPOOL_REAL
                clwb(lp);
                sfence();
                #endif

                #if defined(NVMPOOL_REAL) && defined(UNLOCK_AFTER)
                ((bleafMeta *)lp)->v.lock = 0;
                #endif

                return true;
            }

            // 1.4 line 1--3
            else
            {
            #ifdef ENTRY_MOVING
                int last_slot = last_slot_in_line[slot];
                int from = 0;
                for (int to = slot + 1; to <= last_slot; to++)
                {
                    if ((bitmap & (1 << to)) == 0)
                    {
                        // 1.4.1 for each empty slot in the line
                        // copy an entry from line 0
                        lp->ent[to] = lp->ent[from];
                        meta.v.fgpt[to] = meta.v.fgpt[from];
                        bitmap |= (1 << to);
                        bitmap &= ~(1 << from);
                        from++;
                    }
                }
            #endif

                // 1.4.2 flush the line containing slot
                #ifdef NVMPOOL_REAL
                clwb(&(lp->k(slot)));
                sfence();
                #endif

                // 1.4.3 change meta and flush line 0
                meta.v.bitmap = bitmap;
                #if defined(NVMPOOL_REAL) && defined(NONTEMP) 
                lp->setBothWords_temporal(&meta);
                #else
                lp->setBothWords(&meta);
                #endif
                
                #ifdef NVMPOOL_REAL
                clwb(lp);
                sfence();
                #endif

                #if defined(NVMPOOL_REAL) && defined(UNLOCK_AFTER)
                ((bleafMeta *)lp)->v.lock = 0;
                #endif

                return true;
            }
        } // end of not full

        /* 2. leaf is full, split */

        // 2.1 get sorted positions
        int sorted_pos[LEAF_KEY_NUM];
        for (int i = 0; i < LEAF_KEY_NUM; i++)
            sorted_pos[i] = i;
        qsortBleaf(lp, 0, LEAF_KEY_NUM - 1, sorted_pos);

        // 2.2 split point is the middle point
        int split = (LEAF_KEY_NUM / 2); // [0,..split-1] [split,LEAF_KEY_NUM-1]
        key_type split_key = lp->k(sorted_pos[split]);

        // 2.3 create new node
        bleaf *newp = (bleaf *)nvmpool_alloc_node(LEAF_SIZE);

        // 2.4 move entries sorted_pos[split .. LEAF_KEY_NUM-1]
        uint16_t freed_slots = 0;
        for (int i = split; i < LEAF_KEY_NUM; i++)
        {
            newp->ent[i] = lp->ent[sorted_pos[i]];
            newp->fgpt[i] = lp->fgpt[sorted_pos[i]];

            // add to freed slots bitmap
            freed_slots |= (1 << sorted_pos[i]);
        }
        newp->bitmap = (((1 << (LEAF_KEY_NUM - split)) - 1) << split);
        newp->lock = 0;
        newp->alt = 0;

        // remove freed slots from temp bitmap
        meta.v.bitmap &= ~freed_slots;

        newp->next[0] = lp->next[lp->alt];
        lp->next[1 - lp->alt] = newp;

        // set alt in temp bitmap
        meta.v.alt = 1 - lp->alt;

        // 2.5 key > split_key: insert key into new node
    #ifdef VAR_KEY
        if (vkcmp((char*)key, (char*)split_key) < 0)
    #else
        if (key > split_key)
    #endif
        {
            newp->k(split - 1) = key;
            newp->ch(split - 1) = ptr;
            newp->fgpt[split - 1] = key_hash;
            newp->bitmap |= 1 << (split - 1);

            if (tree_meta->root_level > 0)
                meta.v.lock = 0; // do not clear lock of root
        }
    
        // 2.6 clwb newp, clwb lp line[3] and sfence
        #ifdef NVMPOOL_REAL
        LOOP_FLUSH(clwb, newp, LEAF_LINE_NUM);
        clwb(&(lp->next[0]));
        sfence();
        #endif

        // 2.7 clwb lp and flush: NVM atomic write to switch alt and set bitmap
        lp->setBothWords(&meta);
        #ifdef NVMPOOL_REAL
        clwb(lp);
        sfence();
        #endif

        // 2.8 key < split_key: insert key into old node
    #ifdef VAR_KEY
        if (vkcmp((char*)key, (char*)split_key) >= 0)
    #else
        if (key <= split_key)
    #endif
        {

            // note: lock bit is still set
            if (tree_meta->root_level > 0)
                meta.v.lock = 0; // do not clear lock of root

            // get first empty slot
            uint16_t bitmap = meta.v.bitmap;
            int slot = bitScan(~bitmap) - 1;

            // set leaf.entry[slot]= (k, v);
            // set fgpt, bitmap in meta
            lp->k(slot) = key;
            lp->ch(slot) = ptr;
            meta.v.fgpt[slot] = key_hash;
            bitmap |= (1 << slot);

            // line 0: 0-2; line 1: 3-6; line 2: 7-10; line 3: 11-13
            // in line 0?
            if (slot < 3)
            {
                // write word 0
                meta.v.bitmap = bitmap;
                lp->setWord0(&meta);
                // flush
                #ifdef NVMPOOL_REAL
                clwb(lp);
                sfence();
                #endif
            }
            // line 1--3
            else
            {
            #ifdef ENTRY_MOVING
                int last_slot = last_slot_in_line[slot];
                int from = 0;
                for (int to = slot + 1; to <= last_slot; to++)
                {
                    if ((bitmap & (1 << to)) == 0)
                    {
                        // for each empty slot in the line
                        // copy an entry from line 0
                        lp->ent[to] = lp->ent[from];
                        meta.v.fgpt[to] = meta.v.fgpt[from];
                        bitmap |= (1 << to);
                        bitmap &= ~(1 << from);
                        from++;
                    }
                }
            #endif

                // flush the line containing slot
                #ifdef NVMPOOL_REAL
                clwb(&(lp->k(slot)));
                sfence();
                #endif

                // change meta and flush line 0
                meta.v.bitmap = bitmap;
                lp->setBothWords(&meta);
                #ifdef NVMPOOL_REAL
                clwb(lp);
                sfence();
                #endif
            }
        }

        key = split_key;
        ptr = newp;
        /* (key, ptr) to be inserted in the parent non-leaf */

    } // end of Part 2

    /* Part 3. nonleaf node */
    {
        bnode *p, *newp;
        int n, i, pos, r, lev, total_level;

#define LEFT_KEY_NUM ((NON_LEAF_KEY_NUM) / 2)
#define RIGHT_KEY_NUM ((NON_LEAF_KEY_NUM)-LEFT_KEY_NUM)

        total_level = tree_meta->root_level;
        lev = 1;

        while (lev <= total_level)
        {

            p = parray[lev];
            n = p->num();
            pos = ppos[lev] + 1; // the new child is ppos[lev]+1 >= 1

            /* if the non-leaf is not full, simply insert key ptr */

            if (n < NON_LEAF_KEY_NUM)
            {
                for (i = n; i >= pos; i--)
                    p->ent[i + 1] = p->ent[i];

                p->k(pos) = key;
                p->ch(pos) = ptr;
                p->num() = n + 1;
                #ifdef NVMPOOL_REAL
                sfence();
                #endif

                // unlock after all changes are globally visible
                p->lock() = 0;
                return true;
            }

            /* otherwise allocate a new non-leaf and redistribute the keys */
            newp = (bnode *)mempool_alloc_node(NONLEAF_SIZE);

            /* if key should be in the left node */
            if (pos <= LEFT_KEY_NUM)
            {
                for (r = RIGHT_KEY_NUM, i = NON_LEAF_KEY_NUM; r >= 0; r--, i--)
                {
                    newp->ent[r] = p->ent[i];
                }
                /* newp->key[0] actually is the key to be pushed up !!! */
                for (i = LEFT_KEY_NUM - 1; i >= pos; i--)
                    p->ent[i + 1] = p->ent[i];

                p->k(pos) = key;
                p->ch(pos) = ptr;
            }
            /* if key should be in the right node */
            else
            {
                for (r = RIGHT_KEY_NUM, i = NON_LEAF_KEY_NUM; i >= pos; i--, r--)
                {
                    newp->ent[r] = p->ent[i];
                }
                newp->k(r) = key;
                newp->ch(r) = ptr;
                r--;
                for (; r >= 0; r--, i--)
                {
                    newp->ent[r] = p->ent[i];
                }
            } /* end of else */

            key = newp->k(0);
            ptr = newp;

            p->num() = LEFT_KEY_NUM;
            if (lev < total_level)
                p->lock() = 0; // do not clear lock bit of root
            newp->num() = RIGHT_KEY_NUM;
            newp->lock() = 0;

            lev++;
        } /* end of while loop */

        /* root was splitted !! add another level */
        newp = (bnode *)mempool_alloc_node(NONLEAF_SIZE);

        newp->num() = 1;
        newp->lock() = 1;
        newp->ch(0) = tree_meta->tree_root;
        newp->ch(1) = ptr;
        newp->k(1) = key;
        #ifdef NVMPOOL_REAL
        sfence(); // ensure new node is consistent
        #endif

        void *old_root = tree_meta->tree_root;
        tree_meta->root_level = lev;
        tree_meta->tree_root = newp;
        #ifdef NVMPOOL_REAL
        sfence(); // tree root change is globablly visible
        #endif    // old root and new root are both locked

        // unlock old root
        if (total_level > 0)
        { // previous root is a nonleaf
            ((bnode *)old_root)->lock() = 0;
        }
        else
        { // previous root is a leaf
            ((bleaf *)old_root)->lock = 0;
        }

        // unlock new root
        newp->lock() = 0;

        return true;

#undef RIGHT_KEY_NUM
#undef LEFT_KEY_NUM
    }
}